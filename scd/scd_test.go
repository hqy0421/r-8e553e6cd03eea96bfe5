package scd

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

// loadRecords builds a frame from header+rows records, forcing the given
// column types when type inference would be ambiguous.
func loadRecords(t *testing.T, recs [][]string, types map[string]series.Type) dataframe.DataFrame {
	t.Helper()
	var df dataframe.DataFrame
	if types != nil {
		df = dataframe.LoadRecords(recs, dataframe.WithTypes(types))
	} else {
		df = dataframe.LoadRecords(recs)
	}
	if err := df.Error(); err != nil {
		t.Fatalf("failed to load frame: %v\nrecords: %v", err, recs)
	}
	return df
}

func testConfig() Config {
	return Config{
		DimKeyColumns:  []string{"cust_id"},
		DimTimeColumn:  "eff",
		FactIDColumn:   "order_id",
		FactKeyColumns: []string{"cust_id"},
		FactTimeColumn: "ts",
	}
}

// colVals returns a column as []interface{}, with nil marking NA cells.
func colVals(t *testing.T, df dataframe.DataFrame, name string) []interface{} {
	t.Helper()
	if err := df.Error(); err != nil {
		t.Fatalf("frame has error: %v", df.Error())
	}
	out := make([]interface{}, df.Nrow())
	for i, m := range df.Maps() {
		out[i] = m[name]
	}
	return out
}

func assertDeep(t *testing.T, name string, got, want interface{}) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s:\nwant: %#v\ngot:  %#v", name, want, got)
	}
}

func TestNewProcessor_InvalidConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"no dimension key", Config{DimTimeColumn: "eff"}},
		{"no dimension time", Config{DimKeyColumns: []string{"cust_id"}}},
		{"duplicated dimension columns", Config{DimKeyColumns: []string{"cust_id", "cust_id"}, DimTimeColumn: "eff"}},
		{"time collides with key", Config{DimKeyColumns: []string{"eff"}, DimTimeColumn: "eff"}},
		{"reserved dimension column", Config{DimKeyColumns: []string{"version_key"}, DimTimeColumn: "eff"}},
		{"partial fact config", Config{
			DimKeyColumns: []string{"cust_id"}, DimTimeColumn: "eff",
			FactIDColumn: "order_id", FactTimeColumn: "ts",
		}},
		{"key count mismatch", Config{
			DimKeyColumns: []string{"a", "b"}, DimTimeColumn: "eff",
			FactIDColumn: "id", FactKeyColumns: []string{"a"}, FactTimeColumn: "ts",
		}},
		{"duplicated fact columns", Config{
			DimKeyColumns: []string{"cust_id"}, DimTimeColumn: "eff",
			FactIDColumn: "ts", FactKeyColumns: []string{"cust_id"}, FactTimeColumn: "ts",
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := NewProcessor(c.cfg); err == nil {
				t.Errorf("expected error for %s", c.name)
			}
		})
	}
}

func TestProcessor_BasicLifecycle(t *testing.T) {
	p, err := NewProcessor(testConfig())
	if err != nil {
		t.Fatal(err)
	}

	dims := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "10", "A", "X"},
		{"c1", "20", "B", "X"},
		{"c2", "10", "C", "Y"},
	}, nil)
	if err := p.ApplyDimensions(dims); err != nil {
		t.Fatal(err)
	}

	d := p.Dimensions()
	assertDeep(t, "dim names", d.Names(),
		[]string{"cust_id", "version_key", "start_time", "end_time", "city", "tier"})
	assertDeep(t, "dim cust_id", colVals(t, d, "cust_id"),
		[]interface{}{"c1", "c1", "c2"})
	assertDeep(t, "dim version_key", colVals(t, d, "version_key"),
		[]interface{}{1, 2, 3})
	assertDeep(t, "dim start_time", colVals(t, d, "start_time"),
		[]interface{}{10, 20, 10})
	assertDeep(t, "dim end_time", colVals(t, d, "end_time"),
		[]interface{}{20, nil, nil})
	assertDeep(t, "dim city", colVals(t, d, "city"),
		[]interface{}{"A", "B", "C"})
	assertDeep(t, "dim tier", colVals(t, d, "tier"),
		[]interface{}{"X", "X", "Y"})

	facts := loadRecords(t, [][]string{
		{"order_id", "cust_id", "ts", "amount"},
		{"o1", "c1", "5", "100"},
		{"o2", "c1", "10", "200"},
		{"o3", "c1", "15", "300"},
		{"o4", "c1", "20", "400"},
		{"o5", "c1", "99", "500"},
		{"o6", "c2", "11", "600"},
		{"o7", "c3", "12", "700"},
	}, nil)
	if err := p.ApplyFacts(facts); err != nil {
		t.Fatal(err)
	}

	f := p.Facts()
	assertDeep(t, "fact names", f.Names(),
		[]string{"order_id", "cust_id", "ts", "version_key", "status", "city", "tier", "amount"})
	assertDeep(t, "fact order_id", colVals(t, f, "order_id"),
		[]interface{}{"o1", "o2", "o3", "o4", "o5", "o6", "o7"})
	// o1 precedes the first version, o7 references an unknown key: both stay
	// unmatched; o4 lands exactly on a boundary and joins the new version
	// because intervals are half-open.
	assertDeep(t, "fact version_key", colVals(t, f, "version_key"),
		[]interface{}{nil, 1, 1, 2, 2, 3, nil})
	assertDeep(t, "fact status", colVals(t, f, "status"),
		[]interface{}{"unmatched", "matched", "matched", "matched", "matched", "matched", "unmatched"})
	assertDeep(t, "fact city", colVals(t, f, "city"),
		[]interface{}{nil, "A", "A", "B", "B", "C", nil})
	assertDeep(t, "fact tier", colVals(t, f, "tier"),
		[]interface{}{nil, "X", "X", "X", "X", "Y", nil})
	assertDeep(t, "fact amount", colVals(t, f, "amount"),
		[]interface{}{100, 200, 300, 400, 500, 600, 700})
}

func TestProcessor_MergeAdjacent(t *testing.T) {
	p, _ := NewProcessor(testConfig())

	// Two consecutive events with identical attributes collapse into one
	// version whose start is the earlier effective time.
	dims := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "10", "A", "X"},
		{"c1", "20", "A", "X"},
	}, nil)
	if err := p.ApplyDimensions(dims); err != nil {
		t.Fatal(err)
	}
	d := p.Dimensions()
	assertDeep(t, "merged rows", d.Nrow(), 1)
	assertDeep(t, "merged start", colVals(t, d, "start_time"), []interface{}{10})
	assertDeep(t, "merged end", colVals(t, d, "end_time"), []interface{}{nil})
	assertDeep(t, "merged key", colVals(t, d, "version_key"), []interface{}{1})

	// A later different attribute splits the open version while preserving the
	// original version key and start time.
	next := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "30", "B", "X"},
	}, nil)
	if err := p.ApplyDimensions(next); err != nil {
		t.Fatal(err)
	}
	d = p.Dimensions()
	assertDeep(t, "split version_key", colVals(t, d, "version_key"), []interface{}{1, 2})
	assertDeep(t, "split start_time", colVals(t, d, "start_time"), []interface{}{10, 30})
	assertDeep(t, "split end_time", colVals(t, d, "end_time"), []interface{}{30, nil})
	assertDeep(t, "split city", colVals(t, d, "city"), []interface{}{"A", "B"})
}

func TestProcessor_LateCorrection(t *testing.T) {
	p, _ := NewProcessor(testConfig())
	base := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "10", "A", "X"},
		{"c1", "30", "C", "Z"},
	}, nil)
	if err := p.ApplyDimensions(base); err != nil {
		t.Fatal(err)
	}
	facts := loadRecords(t, [][]string{
		{"order_id", "cust_id", "ts", "amount"},
		{"o1", "c1", "20", "100"},
		{"o2", "c1", "30", "200"},
	}, nil)
	if err := p.ApplyFacts(facts); err != nil {
		t.Fatal(err)
	}
	assertDeep(t, "before correction vk", colVals(t, p.Facts(), "version_key"),
		[]interface{}{1, 2})

	// Late event inserted between two existing starts.
	late := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "20", "B", "X"},
	}, nil)
	if err := p.ApplyDimensions(late); err != nil {
		t.Fatal(err)
	}

	d := p.Dimensions()
	assertDeep(t, "late version_key", colVals(t, d, "version_key"),
		[]interface{}{1, 3, 2})
	assertDeep(t, "late start_time", colVals(t, d, "start_time"),
		[]interface{}{10, 20, 30})
	assertDeep(t, "late end_time", colVals(t, d, "end_time"),
		[]interface{}{20, 30, nil})
	assertDeep(t, "late city", colVals(t, d, "city"),
		[]interface{}{"A", "B", "C"})

	// o1 is re-pointed at the inserted version while o2 keeps its old key.
	f := p.Facts()
	assertDeep(t, "after correction vk", colVals(t, f, "version_key"),
		[]interface{}{3, 2})
	assertDeep(t, "after correction city", colVals(t, f, "city"),
		[]interface{}{"B", "C"})
}

func TestProcessor_Revocation(t *testing.T) {
	cfg := testConfig()
	cfg.DimOpColumn = "op"
	p, _ := NewProcessor(cfg)
	base := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "10", "A", "X"},
		{"c1", "20", "B", "X"},
		{"c1", "30", "C", "Z"},
	}, nil)
	if err := p.ApplyDimensions(base); err != nil {
		t.Fatal(err)
	}
	facts := loadRecords(t, [][]string{
		{"order_id", "cust_id", "ts", "amount"},
		{"o0", "c1", "5", "10"},
		{"o1", "c1", "15", "100"},
		{"o2", "c1", "25", "200"},
		{"o3", "c1", "35", "300"},
	}, nil)
	if err := p.ApplyFacts(facts); err != nil {
		t.Fatal(err)
	}

	revoke := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier", "op"},
		{"c1", "20", "B", "X", "delete"},
	}, nil)
	if err := p.ApplyDimensions(revoke); err != nil {
		t.Fatal(err)
	}

	d := p.Dimensions()
	// The middle identity disappears; the surviving versions keep their keys.
	assertDeep(t, "revoke version_key", colVals(t, d, "version_key"),
		[]interface{}{1, 3})
	assertDeep(t, "revoke start_time", colVals(t, d, "start_time"),
		[]interface{}{10, 30})
	assertDeep(t, "revoke end_time", colVals(t, d, "end_time"),
		[]interface{}{30, nil})
	assertDeep(t, "revoke city", colVals(t, d, "city"),
		[]interface{}{"A", "C"})

	// o2 used to match the revoked version and is re-associated with the
	// version that now covers time 25.
	f := p.Facts()
	assertDeep(t, "revoke fact vk", colVals(t, f, "version_key"),
		[]interface{}{nil, 1, 1, 3})
	assertDeep(t, "revoke fact status", colVals(t, f, "status"),
		[]interface{}{"unmatched", "matched", "matched", "matched"})
	assertDeep(t, "revoke fact city", colVals(t, f, "city"),
		[]interface{}{nil, "A", "A", "C"})
}

func TestProcessor_RevokeLastEvent(t *testing.T) {
	cfg := testConfig()
	cfg.DimOpColumn = "op"
	p, _ := NewProcessor(cfg)
	base := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "10", "A", "X"},
	}, nil)
	if err := p.ApplyDimensions(base); err != nil {
		t.Fatal(err)
	}
	facts := loadRecords(t, [][]string{
		{"order_id", "cust_id", "ts", "amount"},
		{"o1", "c1", "5", "100"},
		{"o2", "c1", "10", "200"},
	}, nil)
	if err := p.ApplyFacts(facts); err != nil {
		t.Fatal(err)
	}

	revoke := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier", "op"},
		{"c1", "10", "A", "X", "delete"},
	}, nil)
	if err := p.ApplyDimensions(revoke); err != nil {
		t.Fatal(err)
	}

	if n := p.Dimensions().Nrow(); n != 0 {
		t.Errorf("expected empty dimension table after full revocation, got %d rows", n)
	}
	f := p.Facts()
	assertDeep(t, "all unmatched vk", colVals(t, f, "version_key"),
		[]interface{}{nil, nil})
	assertDeep(t, "all unmatched status", colVals(t, f, "status"),
		[]interface{}{"unmatched", "unmatched"})
}

func TestProcessor_AtomicBatch(t *testing.T) {
	cfg := testConfig()
	cfg.DimOpColumn = "op"
	p, _ := NewProcessor(cfg)
	base := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "10", "A", "X"},
	}, nil)
	if err := p.ApplyDimensions(base); err != nil {
		t.Fatal(err)
	}
	before := p.Dimensions().Records()

	// An invalid dimension row and fact rows in the same batch: nothing moves.
	badDims := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier", "op"},
		{"c2", "10", "C", "Y", "frobnicate"},
	}, nil)
	badFacts := loadRecords(t, [][]string{
		{"order_id", "cust_id", "ts", "amount"},
		{"o1", "c1", "10", "100"},
	}, nil)
	if err := p.Apply(badDims, badFacts); err == nil {
		t.Fatal("expected invalid batch to fail")
	}
	assertDeep(t, "state unchanged after invalid batch", p.Dimensions().Records(), before)
	if p.Facts().Nrow() != 0 {
		t.Errorf("facts must not be stored from an invalid batch")
	}

	// A valid combined batch applies both sides together and the fact can
	// resolve the dimension introduced in the same batch.
	goodDims := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier", "op"},
		{"c2", "10", "C", "Y", "upsert"},
	}, nil)
	goodFacts := loadRecords(t, [][]string{
		{"order_id", "cust_id", "ts", "amount"},
		{"o1", "c2", "10", "100"},
	}, nil)
	if err := p.Apply(goodDims, goodFacts); err != nil {
		t.Fatalf("valid batch failed: %v", err)
	}
	f := p.Facts()
	assertDeep(t, "same-batch association", colVals(t, f, "version_key"),
		[]interface{}{2})
	assertDeep(t, "same-batch city", colVals(t, f, "city"),
		[]interface{}{"C"})
}

func TestProcessor_StableVersionKeyUnaffectedKey(t *testing.T) {
	p, _ := NewProcessor(testConfig())
	base := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "10", "A", "X"},
		{"c1", "20", "B", "X"},
		{"c2", "10", "C", "Y"},
	}, nil)
	if err := p.ApplyDimensions(base); err != nil {
		t.Fatal(err)
	}
	c1Before := p.Dimensions().Subset([]int{0, 1}).Records()

	update := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c2", "20", "D", "Y"},
	}, nil)
	if err := p.ApplyDimensions(update); err != nil {
		t.Fatal(err)
	}
	c1After := p.Dimensions().Subset([]int{0, 1}).Records()
	assertDeep(t, "untouched key stable", c1After, c1Before)
}

func TestProcessor_FactChanges(t *testing.T) {
	p, _ := NewProcessor(testConfig())
	dims := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "10", "A", "X"},
		{"c1", "20", "B", "X"},
		{"c2", "10", "C", "Y"},
	}, nil)
	if err := p.ApplyDimensions(dims); err != nil {
		t.Fatal(err)
	}
	order := loadRecords(t, [][]string{
		{"order_id", "cust_id", "ts", "amount"},
		{"o1", "c1", "15", "100"},
	}, nil)
	if err := p.ApplyFacts(order); err != nil {
		t.Fatal(err)
	}
	assertDeep(t, "initial vk", colVals(t, p.Facts(), "version_key"),
		[]interface{}{1})

	// Re-upload the same order under another business key.
	moved := loadRecords(t, [][]string{
		{"order_id", "cust_id", "ts", "amount"},
		{"o1", "c2", "15", "999"},
	}, nil)
	if err := p.ApplyFacts(moved); err != nil {
		t.Fatal(err)
	}
	f := p.Facts()
	assertDeep(t, "moved vk", colVals(t, f, "version_key"), []interface{}{3})
	assertDeep(t, "moved key", colVals(t, f, "cust_id"), []interface{}{"c2"})
	assertDeep(t, "moved amount", colVals(t, f, "amount"), []interface{}{999})
	if f.Nrow() != 1 {
		t.Fatalf("re-upload must replace, got %d rows", f.Nrow())
	}

	// Deleting an unknown id is a no-op; deleting the real one removes it.
	cfg2 := testConfig()
	cfg2.FactOpColumn = "op"
	p2, err := NewProcessor(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	if err := p2.ApplyDimensions(dims); err != nil {
		t.Fatal(err)
	}
	if err := p2.ApplyFacts(order); err != nil {
		t.Fatal(err)
	}
	if err := p2.ApplyFacts(moved); err != nil {
		t.Fatal(err)
	}
	unknownDelete := loadRecords(t, [][]string{
		{"order_id", "cust_id", "ts", "amount", "op"},
		{"oX", "c2", "15", "1", "delete"},
	}, nil)
	if err := p2.ApplyFacts(unknownDelete); err != nil {
		t.Fatalf("unknown id deletion should be accepted: %v", err)
	}
	if p2.Facts().Nrow() != 1 {
		t.Fatal("unknown id deletion must not change the fact table")
	}
	realDelete := loadRecords(t, [][]string{
		{"order_id", "cust_id", "ts", "amount", "op"},
		{"o1", "c2", "15", "999", "DELETE"},
	}, nil)
	if err := p2.ApplyFacts(realDelete); err != nil {
		t.Fatal(err)
	}
	if p2.Facts().Nrow() != 0 {
		t.Fatal("fact row was not deleted")
	}
}

func TestProcessor_CompositeKeyAndStringTime(t *testing.T) {
	cfg := Config{
		DimKeyColumns:  []string{"region", "cust_id"},
		DimTimeColumn:  "eff",
		FactIDColumn:   "order_id",
		FactKeyColumns: []string{"region", "cust_id"},
		FactTimeColumn: "ts",
	}
	p, err := NewProcessor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	dims := loadRecords(t, [][]string{
		{"region", "cust_id", "eff", "city"},
		{"eu", "c1", "2026-01-01", "A"},
		{"eu", "c1", "2026-02-01", "B"},
		{"us", "c1", "2026-01-01", "C"},
	}, nil)
	if err := p.ApplyDimensions(dims); err != nil {
		t.Fatal(err)
	}
	d := p.Dimensions()
	assertDeep(t, "composite key names", d.Names(),
		[]string{"region", "cust_id", "version_key", "start_time", "end_time", "city"})
	assertDeep(t, "composite regions", colVals(t, d, "region"),
		[]interface{}{"eu", "eu", "us"})
	assertDeep(t, "composite starts", colVals(t, d, "start_time"),
		[]interface{}{"2026-01-01", "2026-02-01", "2026-01-01"})

	facts := loadRecords(t, [][]string{
		{"order_id", "region", "cust_id", "ts", "amount"},
		{"o1", "eu", "c1", "2026-01-15", "10"},
		{"o2", "eu", "c1", "2026-02-01", "20"},
		{"o3", "us", "c1", "2026-03-01", "30"},
	}, nil)
	if err := p.ApplyFacts(facts); err != nil {
		t.Fatal(err)
	}
	assertDeep(t, "composite fact vk", colVals(t, p.Facts(), "version_key"),
		[]interface{}{1, 2, 3})
	assertDeep(t, "composite fact city", colVals(t, p.Facts(), "city"),
		[]interface{}{"A", "B", "C"})
}

func TestProcessor_NAAttributesAndMandatoryColumns(t *testing.T) {
	p, _ := NewProcessor(testConfig())
	dims := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "10", "A", "NaN"},
		{"c1", "20", "A", "NaN"},
		{"c1", "30", "B", "X"},
	}, nil)
	if err := p.ApplyDimensions(dims); err != nil {
		t.Fatal(err)
	}
	d := p.Dimensions()
	// NA attributes compare equal, so the first two events merge.
	assertDeep(t, "NA merge keys", colVals(t, d, "version_key"), []interface{}{1, 2})
	assertDeep(t, "NA tier exported", colVals(t, d, "tier"), []interface{}{nil, "X"})

	facts := loadRecords(t, [][]string{
		{"order_id", "cust_id", "ts", "amount"},
		{"o1", "c1", "15", "100"},
	}, nil)
	if err := p.ApplyFacts(facts); err != nil {
		t.Fatal(err)
	}
	assertDeep(t, "NA fact tier", colVals(t, p.Facts(), "tier"), []interface{}{nil})

	// NA in a mandatory column rejects the whole batch.
	bad := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"NaN", "40", "D", "Y"},
	}, nil)
	if err := p.ApplyDimensions(bad); err == nil {
		t.Fatal("expected error for NA business key")
	}
	badTime := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "NaN", "D", "Y"},
	}, nil)
	if err := p.ApplyDimensions(badTime); err == nil {
		t.Fatal("expected error for NA effective time")
	}
	// The rejected batches must not have added a version.
	assertDeep(t, "rows after rejected batches", p.Dimensions().Nrow(), 2)
}

func TestProcessor_SchemaLock(t *testing.T) {
	p, _ := NewProcessor(testConfig())
	base := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "10", "A", "X"},
	}, nil)
	if err := p.ApplyDimensions(base); err != nil {
		t.Fatal(err)
	}

	// Attribute column removed.
	missingCol := loadRecords(t, [][]string{
		{"cust_id", "eff", "city"},
		{"c1", "20", "B"},
	}, nil)
	if err := p.ApplyDimensions(missingCol); err == nil {
		t.Fatal("expected error when attribute schema shrinks")
	}

	// Attribute type changed.
	wrongType := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "20", "B", "1"},
	}, map[string]series.Type{"tier": series.Int})
	if err := p.ApplyDimensions(wrongType); err == nil {
		t.Fatal("expected error when attribute type changes")
	}

	// A failed first batch must not lock the schema: a well-formed batch later
	// still works.
	q, _ := NewProcessor(testConfig())
	badFirst := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "NaN", "A", "X"},
	}, nil)
	if err := q.ApplyDimensions(badFirst); err == nil {
		t.Fatal("expected first batch to fail")
	}
	good := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "10", "A", "X"},
	}, nil)
	if err := q.ApplyDimensions(good); err != nil {
		t.Fatalf("schema should not be locked by failed batch: %v", err)
	}

	// Fact time type must agree with the dimension time type.
	factWrongTime := loadRecords(t, [][]string{
		{"order_id", "cust_id", "ts", "amount"},
		{"o1", "c1", "2026-01-01", "100"},
	}, nil)
	if err := p.ApplyFacts(factWrongTime); err == nil {
		t.Fatal("expected fact/dimension time type mismatch to fail")
	}
}

func TestProcessor_ReservedColumns(t *testing.T) {
	p, _ := NewProcessor(testConfig())
	bad := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "version_key"},
		{"c1", "10", "A", "X"},
	}, nil)
	if err := p.ApplyDimensions(bad); err == nil {
		t.Fatal("expected reserved column error")
	}
}

func TestProcessor_DuplicateEventInBatch(t *testing.T) {
	p, _ := NewProcessor(testConfig())
	// Same business key and time twice: the last row wins.
	dims := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "10", "A", "X"},
		{"c1", "10", "B", "Y"},
	}, nil)
	if err := p.ApplyDimensions(dims); err != nil {
		t.Fatal(err)
	}
	d := p.Dimensions()
	assertDeep(t, "dedup rows", d.Nrow(), 1)
	assertDeep(t, "last row wins city", colVals(t, d, "city"), []interface{}{"B"})
	assertDeep(t, "last row wins tier", colVals(t, d, "tier"), []interface{}{"Y"})
}

func TestProcessor_CustomOperations(t *testing.T) {
	cfg := testConfig()
	cfg.DimOpColumn = "op"
	cfg.FactOpColumn = "fop"
	cfg.UpsertValues = []string{"INS"}
	cfg.DeleteValues = []string{"DEL"}
	p, err := NewProcessor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	dims := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier", "op"},
		{"c1", "10", "A", "X", "INS"},
		{"c1", "20", "B", "X", "ins"},
	}, nil)
	if err := p.ApplyDimensions(dims); err != nil {
		t.Fatal(err)
	}
	if n := p.Dimensions().Nrow(); n != 2 {
		t.Fatalf("expected 2 versions, got %d", n)
	}
	revoke := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier", "op"},
		{"c1", "20", "B", "X", "DEL"},
	}, nil)
	if err := p.ApplyDimensions(revoke); err != nil {
		t.Fatal(err)
	}
	if n := p.Dimensions().Nrow(); n != 1 {
		t.Fatalf("expected 1 version after DEL, got %d", n)
	}

	facts := loadRecords(t, [][]string{
		{"order_id", "cust_id", "ts", "amount", "fop"},
		{"o1", "c1", "10", "100", "INS"},
	}, nil)
	if err := p.ApplyFacts(facts); err != nil {
		t.Fatal(err)
	}
	if n := p.Facts().Nrow(); n != 1 {
		t.Fatalf("expected 1 fact, got %d", n)
	}
}

func TestProcessor_DimensionOnlyAndEmptyExports(t *testing.T) {
	p, err := NewProcessor(Config{
		DimKeyColumns: []string{"cust_id"},
		DimTimeColumn: "eff",
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := p.Dimensions().Nrow(); n != 0 {
		t.Fatalf("empty export should have 0 rows, got %d", n)
	}
	if n := p.Facts().Nrow(); n != 0 {
		t.Fatalf("unconfigured fact export should have 0 rows, got %d", n)
	}
	facts := loadRecords(t, [][]string{
		{"order_id", "cust_id", "ts"},
		{"o1", "c1", "10"},
	}, nil)
	if err := p.ApplyFacts(facts); err == nil {
		t.Fatal("dimension-only processor must reject fact batches")
	}
}

func TestProcessor_ExportIsNativeDataFrame(t *testing.T) {
	p, _ := NewProcessor(testConfig())
	dims := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "10", "A", "X"},
		{"c1", "20", "B", "X"},
	}, nil)
	if err := p.ApplyDimensions(dims); err != nil {
		t.Fatal(err)
	}
	facts := loadRecords(t, [][]string{
		{"order_id", "cust_id", "ts", "amount"},
		{"o1", "c1", "5", "100"},
		{"o2", "c1", "10", "200"},
		{"o3", "c1", "20", "300"},
	}, nil)
	if err := p.ApplyFacts(facts); err != nil {
		t.Fatal(err)
	}

	// The maintained version_key supports ordinary equality joins: attach the
	// version interval to every matched fact through the native InnerJoin.
	d := p.Dimensions()
	f := p.Facts()
	intervals := d.Select([]string{"version_key", "start_time", "end_time"})
	joined := f.InnerJoin(intervals, "version_key")
	if err := joined.Error(); err != nil {
		t.Fatalf("join failed: %v", err)
	}
	if joined.Nrow() != 2 {
		t.Fatalf("expected 2 matched join rows, got %d", joined.Nrow())
	}

	// Both exports survive the native CSV writer round trip.
	var buf bytes.Buffer
	if err := p.Dimensions().WriteCSV(&buf); err != nil {
		t.Fatalf("dim WriteCSV: %v", err)
	}
	buf.Reset()
	if err := p.Facts().WriteCSV(&buf); err != nil {
		t.Fatalf("fact WriteCSV: %v", err)
	}
}

func TestProcessor_Reset(t *testing.T) {
	p, _ := NewProcessor(testConfig())
	dims := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "10", "A", "X"},
	}, nil)
	if err := p.ApplyDimensions(dims); err != nil {
		t.Fatal(err)
	}
	p.Reset()
	if p.Dimensions().Nrow() != 0 || p.Facts().Nrow() != 0 {
		t.Fatal("Reset must empty both exports")
	}
	// Version numbering restarts after reset.
	if err := p.ApplyDimensions(dims); err != nil {
		t.Fatal(err)
	}
	assertDeep(t, "counter restarts", colVals(t, p.Dimensions(), "version_key"),
		[]interface{}{1})
}

func TestProcessor_FloatTime(t *testing.T) {
	p, _ := NewProcessor(testConfig())
	dims := loadRecords(t, [][]string{
		{"cust_id", "eff", "city", "tier"},
		{"c1", "1.5", "A", "X"},
		{"c1", "2.5", "B", "X"},
	}, map[string]series.Type{"eff": series.Float})
	if err := p.ApplyDimensions(dims); err != nil {
		t.Fatal(err)
	}
	facts := loadRecords(t, [][]string{
		{"order_id", "cust_id", "ts", "amount"},
		{"o1", "c1", "2.5", "100"},
		{"o2", "c1", "2.4", "200"},
	}, map[string]series.Type{"ts": series.Float})
	if err := p.ApplyFacts(facts); err != nil {
		t.Fatal(err)
	}
	// Half-open boundary: 2.5 belongs to the second version, 2.4 to the first.
	assertDeep(t, "float fact vk", colVals(t, p.Facts(), "version_key"),
		[]interface{}{2, 1})
	assertDeep(t, "float end_time", colVals(t, p.Dimensions(), "end_time"),
		[]interface{}{2.5, nil})
}
