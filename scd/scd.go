// Package scd implements slowly changing dimension (SCD type 2) maintenance on
// top of the gota dataframe/series primitives.
//
// A Processor keeps the full history of dimension attribute events grouped by
// business key, materializes them into half-open [start_time, end_time)
// versions, and keeps an order fact table pointing at the version that was in
// effect at each order's business time. Because the gota join operators only
// support equality comparisons, the temporal association cannot be expressed
// with a regular join and is maintained directly by this package.
//
// The processor is stateful: batches of dimension events and/or fact rows are
// applied incrementally. Late corrections, historical revocations and fact
// changes only recompute the affected business keys, while version keys stay
// stable for every version whose business key and start time did not change.
// All validation of a batch happens before any state mutation, so an invalid
// batch never modifies the maintained dimension or fact data.
package scd

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

// Names of the columns produced by the exported dimension and fact tables.
const (
	VersionKeyColumn = "version_key"
	StartTimeColumn  = "start_time"
	EndTimeColumn    = "end_time"
	StatusColumn     = "status"
)

// Fact association statuses produced by the exported fact table.
const (
	StatusMatched   = "matched"
	StatusUnmatched = "unmatched"
)

// Default operation values recognized in the optional operation columns.
// Matching is case-insensitive.
var (
	defaultUpsertValues = []string{"upsert", "u", "insert", "update"}
	defaultDeleteValues = []string{"delete", "d", "revoke", "remove"}
)

// reservedColumns are produced by Dimensions and Facts and therefore cannot be
// used as input column names.
var reservedColumns = map[string]bool{
	VersionKeyColumn: true,
	StartTimeColumn:  true,
	EndTimeColumn:    true,
	StatusColumn:     true,
}

// Config configures a Processor.
//
// Dimension frames are described by DimKeyColumns (the business key),
// DimTimeColumn (the effective time of the attribute event) and the optional
// DimOpColumn, which marks a row as an upsert or a delete/revoke. Every other
// dimension column is treated as a tracked attribute.
//
// Fact frames are described by FactIDColumn (the order identifier),
// FactKeyColumns (the dimension business key referenced by the order, in the
// same order as DimKeyColumns), FactTimeColumn (the order business time) and
// the optional FactOpColumn. Every other fact column is treated as a measure
// and carried through to the exported fact table. Fact processing is optional:
// the fact fields may all be left empty when only dimensions are maintained.
type Config struct {
	DimKeyColumns []string
	DimTimeColumn string
	DimOpColumn   string

	FactIDColumn   string
	FactKeyColumns []string
	FactTimeColumn string
	FactOpColumn   string

	// UpsertValues and DeleteValues override the operation values recognized
	// in the operation columns. Values are matched case-insensitively.
	UpsertValues []string
	DeleteValues []string
}

// schemaCol records the name and series type locked for a tracked column.
type schemaCol struct {
	name string
	t    series.Type
}

// attrSet is a full set of attribute values as of a given effective time.
// nil values represent NA. Two attrSets are equal when every value matches,
// treating NA as equal to NA, so adjacent events carrying identical attributes
// can be merged into a single version.
type attrSet struct {
	values []interface{}
}

func (a attrSet) equal(other attrSet) bool {
	return reflect.DeepEqual(a.values, other.values)
}

// dimEvent is a single dimension attribute event for a business key.
type dimEvent struct {
	keyCanon string
	key      []interface{}
	time     interface{}
	attrs    attrSet
}

// version is a materialized SCD2 interval: [start, end), where a nil end means
// the currently open version.
type version struct {
	start interface{}
	end   interface{} // nil for the open version
	attrs attrSet
	key   int
}

// factRecord is a maintained order row together with its current association.
type factRecord struct {
	id       []interface{}
	idCanon  string
	key      []interface{}
	keyCanon string
	time     interface{}
	measures []interface{}

	matched bool
	vk      int // only meaningful when matched
	attrs   attrSet
}

// identityKey identifies a version by business key and interval start. Version
// keys are bound to this identity, so a version keeps its key as long as its
// business key and start time do not change.
type identityKey struct {
	key  string
	time interface{}
}

// dimMutation and factMutation are the planned, validated effects of one batch.
type dimMutation struct {
	delete bool
	event  *dimEvent
}

type factMutation struct {
	delete bool
	record *factRecord
}

// Processor maintains SCD2 dimension versions and the associated fact table.
// It is safe for sequential use; methods must not be called concurrently.
type Processor struct {
	cfg          Config
	upsertValues map[string]bool
	deleteValues map[string]bool

	// Locked input schemas.
	keyCols     []schemaCol
	timeCol     schemaCol
	attrCols    []schemaCol
	dimLocked   bool
	dimOpColumn string

	idCol        schemaCol
	factKeyCols  []schemaCol
	factTimeCol  schemaCol
	measureCols  []schemaCol
	factLocked   bool
	factOpColumn string
	factTimeType series.Type
	factKeyTypes []series.Type
	factIDType   series.Type

	// Stored event and fact state.
	dimEvents map[string]map[interface{}]*dimEvent
	facts     map[string]*factRecord

	// Materialized versions and the stable version-key registry.
	versions map[string][]version
	identity map[identityKey]int
	counter  int
}

// NewProcessor validates the configuration and returns an empty Processor.
func NewProcessor(cfg Config) (*Processor, error) {
	if len(cfg.DimKeyColumns) == 0 {
		return nil, fmt.Errorf("scd: DimKeyColumns must contain at least one column")
	}
	if cfg.DimTimeColumn == "" {
		return nil, fmt.Errorf("scd: DimTimeColumn must be set")
	}
	if err := checkDistinct("dimension", append(append([]string{}, cfg.DimKeyColumns...), cfg.DimTimeColumn, cfg.DimOpColumn)); err != nil {
		return nil, err
	}

	factSet := cfg.FactIDColumn != "" || len(cfg.FactKeyColumns) > 0 || cfg.FactTimeColumn != ""
	if factSet {
		if cfg.FactIDColumn == "" || cfg.FactTimeColumn == "" || len(cfg.FactKeyColumns) == 0 {
			return nil, fmt.Errorf("scd: fact processing requires FactIDColumn, FactKeyColumns and FactTimeColumn")
		}
		if len(cfg.FactKeyColumns) != len(cfg.DimKeyColumns) {
			return nil, fmt.Errorf("scd: FactKeyColumns must reference the same number of columns as DimKeyColumns")
		}
		if err := checkDistinct("fact", append(append([]string{cfg.FactIDColumn}, cfg.FactKeyColumns...), cfg.FactTimeColumn, cfg.FactOpColumn)); err != nil {
			return nil, err
		}
	}

	upsert := cfg.UpsertValues
	if len(upsert) == 0 {
		upsert = defaultUpsertValues
	}
	del := cfg.DeleteValues
	if len(del) == 0 {
		del = defaultDeleteValues
	}
	p := &Processor{
		cfg:          cfg,
		upsertValues: normalizeOps(upsert),
		deleteValues: normalizeOps(del),
		dimEvents:    map[string]map[interface{}]*dimEvent{},
		facts:        map[string]*factRecord{},
		versions:     map[string][]version{},
		identity:     map[identityKey]int{},
	}
	return p, nil
}

func normalizeOps(values []string) map[string]bool {
	m := make(map[string]bool, len(values))
	for _, v := range values {
		m[strings.ToLower(strings.TrimSpace(v))] = true
	}
	return m
}

func checkDistinct(scope string, columns []string) error {
	seen := map[string]bool{}
	for _, c := range columns {
		if c == "" {
			continue
		}
		if reservedColumns[c] {
			return fmt.Errorf("scd: %s column %q is reserved for scd output", scope, c)
		}
		if seen[c] {
			return fmt.Errorf("scd: duplicated %s column %q", scope, c)
		}
		seen[c] = true
	}
	return nil
}

// frameSchema exposes the columns of a frame by name.
type frameSchema struct {
	names []string
	types []series.Type
	idx   map[string]int
}

func readSchema(df dataframe.DataFrame) (frameSchema, error) {
	if err := df.Error(); err != nil {
		return frameSchema{}, fmt.Errorf("scd: input frame has errors: %v", err)
	}
	names := df.Names()
	types := df.Types()
	idx := make(map[string]int, len(names))
	for i, n := range names {
		if reservedColumns[n] {
			return frameSchema{}, fmt.Errorf("scd: input column %q is reserved for scd output", n)
		}
		if _, dup := idx[n]; dup {
			return frameSchema{}, fmt.Errorf("scd: duplicated input column %q", n)
		}
		idx[n] = i
	}
	return frameSchema{names: names, types: types, idx: idx}, nil
}

func (fs frameSchema) require(col string) (series.Type, error) {
	i, ok := fs.idx[col]
	if !ok {
		return "", fmt.Errorf("scd: required column %q not found", col)
	}
	return fs.types[i], nil
}

// isTimeType reports whether t is supported as an effective-time column.
func isTimeType(t series.Type) bool {
	return t == series.Int || t == series.Float || t == series.String
}

// rowValue returns the typed value of a row cell, with nil representing NA.
func rowValue(row map[string]interface{}, col string) interface{} {
	return row[col]
}

// checkNotNull rejects NA values in mandatory columns.
func checkNotNull(row map[string]interface{}, col string, rowIdx int) error {
	if rowValue(row, col) == nil {
		return fmt.Errorf("scd: NA value in mandatory column %q at row %d", col, rowIdx)
	}
	return nil
}

// canonical renders a tuple of key values into a deterministic, collision-free
// string used as the internal business-key identity.
func canonical(values []interface{}) string {
	var b strings.Builder
	for i, v := range values {
		if i > 0 {
			b.WriteByte(0x1f)
		}
		switch val := v.(type) {
		case nil:
			b.WriteString("n:")
		case string:
			b.WriteString("s:")
			b.WriteString(val)
		case int:
			b.WriteString("i:")
			b.WriteString(strconv.Itoa(val))
		case float64:
			b.WriteString("f:")
			b.WriteString(strconv.FormatFloat(val, 'g', -1, 64))
		case bool:
			b.WriteString("b:")
			b.WriteString(strconv.FormatBool(val))
		default:
			b.WriteString("?:" + fmt.Sprintf("%v", val))
		}
	}
	return b.String()
}

// timeCmp compares two effective-time values of the same type. It returns -1,
// 0 or 1. NA times are rejected during validation.
func timeCmp(a, b interface{}) int {
	switch x := a.(type) {
	case int:
		y := b.(int)
		if x < y {
			return -1
		} else if x > y {
			return 1
		}
		return 0
	case float64:
		y := b.(float64)
		if x < y {
			return -1
		} else if x > y {
			return 1
		}
		return 0
	case string:
		y := b.(string)
		if x < y {
			return -1
		} else if x > y {
			return 1
		}
		return 0
	default:
		panic(fmt.Sprintf("scd: unsupported time type %T", a))
	}
}

// parseDimensions validates a dimension frame and plans its mutations without
// touching the processor state. Rows sharing the same business key and
// effective time are collapsed, with the last row winning deterministically.
func (p *Processor) parseDimensions(df dataframe.DataFrame) (map[string]map[interface{}]*dimMutation, map[string]struct{}, error) {
	fs, err := readSchema(df)
	if err != nil {
		return nil, nil, err
	}

	keyTypes := make([]series.Type, len(p.cfg.DimKeyColumns))
	for i, col := range p.cfg.DimKeyColumns {
		t, err := fs.require(col)
		if err != nil {
			return nil, nil, err
		}
		keyTypes[i] = t
	}
	timeType, err := fs.require(p.cfg.DimTimeColumn)
	if err != nil {
		return nil, nil, err
	}
	if !isTimeType(timeType) {
		return nil, nil, fmt.Errorf("scd: dimension time column %q must be int, float or string, got %s", p.cfg.DimTimeColumn, timeType)
	}
	if p.dimLocked && timeType != p.timeCol.t {
		return nil, nil, fmt.Errorf("scd: dimension time column type changed from %s to %s", p.timeCol.t, timeType)
	}
	if p.factLocked && timeType != p.factTimeType {
		return nil, nil, fmt.Errorf("scd: dimension time type %s does not match fact time type %s", timeType, p.factTimeType)
	}
	if p.dimLocked {
		for i, col := range p.cfg.DimKeyColumns {
			if keyTypes[i] != p.keyCols[i].t {
				return nil, nil, fmt.Errorf("scd: key column %q type changed from %s to %s", col, p.keyCols[i].t, keyTypes[i])
			}
		}
	} else if p.factLocked {
		for i := range keyTypes {
			if keyTypes[i] != p.factKeyTypes[i] {
				return nil, nil, fmt.Errorf("scd: dimension key column %q type %s does not match fact key type %s",
					p.cfg.DimKeyColumns[i], keyTypes[i], p.factKeyTypes[i])
			}
		}
	}

	opCol := p.cfg.DimOpColumn
	if opCol != "" {
		// An operation column may be omitted from a batch, in which case every
		// row is treated as an upsert.
		if i, ok := fs.idx[opCol]; ok {
			if fs.types[i] != series.String {
				return nil, nil, fmt.Errorf("scd: operation column %q must be of type string", opCol)
			}
		} else {
			opCol = ""
		}
	}

	// Attribute columns are every remaining, non-special column, in frame order.
	var attrNames []string
	for _, name := range fs.names {
		if name == opCol || contains(p.cfg.DimKeyColumns, name) || name == p.cfg.DimTimeColumn {
			continue
		}
		attrNames = append(attrNames, name)
	}
	if len(attrNames) == 0 {
		return nil, nil, fmt.Errorf("scd: dimension frame has no attribute columns")
	}
	attrTypes := make([]series.Type, len(attrNames))
	for i, name := range attrNames {
		attrTypes[i] = fs.types[fs.idx[name]]
	}
	if p.dimLocked {
		if err := checkSchemaLock("attribute", p.attrCols, attrNames, attrTypes); err != nil {
			return nil, nil, err
		}
	}

	rows := df.Maps()
	plan := map[string]map[interface{}]*dimMutation{}
	affected := map[string]struct{}{}

	for rowIdx, row := range rows {
		for _, col := range p.cfg.DimKeyColumns {
			if err := checkNotNull(row, col, rowIdx); err != nil {
				return nil, nil, err
			}
		}
		if err := checkNotNull(row, p.cfg.DimTimeColumn, rowIdx); err != nil {
			return nil, nil, err
		}

		isDelete := false
		if opCol != "" {
			op, ok := row[opCol].(string)
			if !ok {
				return nil, nil, fmt.Errorf("scd: NA or invalid operation at row %d", rowIdx)
			}
			switch {
			case p.upsertValues[strings.ToLower(op)]:
			case p.deleteValues[strings.ToLower(op)]:
				isDelete = true
			default:
				return nil, nil, fmt.Errorf("scd: unknown operation %q at row %d", op, rowIdx)
			}
		}

		keys := make([]interface{}, len(p.cfg.DimKeyColumns))
		for i, col := range p.cfg.DimKeyColumns {
			keys[i] = row[col]
		}
		keyCanon := canonical(keys)
		t := row[p.cfg.DimTimeColumn]

		if _, ok := plan[keyCanon]; !ok {
			plan[keyCanon] = map[interface{}]*dimMutation{}
		}
		affected[keyCanon] = struct{}{}

		if isDelete {
			plan[keyCanon][t] = &dimMutation{delete: true}
			continue
		}

		attrs := make([]interface{}, len(attrNames))
		for i, col := range attrNames {
			attrs[i] = row[col]
		}
		plan[keyCanon][t] = &dimMutation{
			event: &dimEvent{
				keyCanon: keyCanon,
				key:      keys,
				time:     t,
				attrs:    attrSet{values: attrs},
			},
		}
	}

	// Lock the dimension schema only once parsing fully succeeded.
	if !p.dimLocked {
		p.keyCols = make([]schemaCol, len(p.cfg.DimKeyColumns))
		for i, col := range p.cfg.DimKeyColumns {
			p.keyCols[i] = schemaCol{name: col, t: keyTypes[i]}
		}
		p.timeCol = schemaCol{name: p.cfg.DimTimeColumn, t: timeType}
		p.attrCols = make([]schemaCol, len(attrNames))
		for i, n := range attrNames {
			p.attrCols[i] = schemaCol{name: n, t: attrTypes[i]}
		}
		p.dimOpColumn = opCol
		p.dimLocked = true
	}
	return plan, affected, nil
}

// parseFacts validates a fact frame and plans its mutations.
func (p *Processor) parseFacts(df dataframe.DataFrame) (map[string]*factMutation, map[string]struct{}, error) {
	fs, err := readSchema(df)
	if err != nil {
		return nil, nil, err
	}

	idType, err := fs.require(p.cfg.FactIDColumn)
	if err != nil {
		return nil, nil, err
	}
	factKeyTypes := make([]series.Type, len(p.cfg.FactKeyColumns))
	for i, col := range p.cfg.FactKeyColumns {
		t, err := fs.require(col)
		if err != nil {
			return nil, nil, err
		}
		factKeyTypes[i] = t
	}
	factTimeType, err := fs.require(p.cfg.FactTimeColumn)
	if err != nil {
		return nil, nil, err
	}
	if !isTimeType(factTimeType) {
		return nil, nil, fmt.Errorf("scd: fact time column %q must be int, float or string, got %s", p.cfg.FactTimeColumn, factTimeType)
	}
	if p.factLocked {
		if factTimeType != p.factTimeCol.t {
			return nil, nil, fmt.Errorf("scd: fact time column type changed from %s to %s", p.factTimeCol.t, factTimeType)
		}
		if idType != p.idCol.t {
			return nil, nil, fmt.Errorf("scd: fact id column type changed from %s to %s", p.idCol.t, idType)
		}
		for i, col := range p.cfg.FactKeyColumns {
			if factKeyTypes[i] != p.factKeyCols[i].t {
				return nil, nil, fmt.Errorf("scd: fact key column %q type changed from %s to %s", col, p.factKeyCols[i].t, factKeyTypes[i])
			}
		}
	}
	if p.dimLocked {
		if factTimeType != p.timeCol.t {
			return nil, nil, fmt.Errorf("scd: fact time type %s does not match dimension time type %s", factTimeType, p.timeCol.t)
		}
		for i := range factKeyTypes {
			if factKeyTypes[i] != p.keyCols[i].t {
				return nil, nil, fmt.Errorf("scd: fact key column %q type %s does not match dimension key type %s",
					p.cfg.FactKeyColumns[i], factKeyTypes[i], p.keyCols[i].t)
			}
		}
	}

	opCol := p.cfg.FactOpColumn
	if opCol != "" {
		// An operation column may be omitted from a batch, in which case every
		// row is treated as an upsert.
		if i, ok := fs.idx[opCol]; ok {
			if fs.types[i] != series.String {
				return nil, nil, fmt.Errorf("scd: operation column %q must be of type string", opCol)
			}
		} else {
			opCol = ""
		}
	}

	var measureNames []string
	special := map[string]bool{
		p.cfg.FactIDColumn:   true,
		p.cfg.FactTimeColumn: true,
		opCol:                true,
	}
	for _, col := range p.cfg.FactKeyColumns {
		special[col] = true
	}
	for _, name := range fs.names {
		if !special[name] {
			measureNames = append(measureNames, name)
		}
	}
	measureTypes := make([]series.Type, len(measureNames))
	for i, name := range measureNames {
		measureTypes[i] = fs.types[fs.idx[name]]
	}
	if p.factLocked {
		if err := checkSchemaLock("measure", p.measureCols, measureNames, measureTypes); err != nil {
			return nil, nil, err
		}
	}

	rows := df.Maps()
	plan := map[string]*factMutation{}
	affected := map[string]struct{}{}

	for rowIdx, row := range rows {
		if err := checkNotNull(row, p.cfg.FactIDColumn, rowIdx); err != nil {
			return nil, nil, err
		}
		for _, col := range p.cfg.FactKeyColumns {
			if err := checkNotNull(row, col, rowIdx); err != nil {
				return nil, nil, err
			}
		}
		if err := checkNotNull(row, p.cfg.FactTimeColumn, rowIdx); err != nil {
			return nil, nil, err
		}

		isDelete := false
		if opCol != "" {
			op, ok := row[opCol].(string)
			if !ok {
				return nil, nil, fmt.Errorf("scd: NA or invalid fact operation at row %d", rowIdx)
			}
			switch {
			case p.upsertValues[strings.ToLower(op)]:
			case p.deleteValues[strings.ToLower(op)]:
				isDelete = true
			default:
				return nil, nil, fmt.Errorf("scd: unknown fact operation %q at row %d", op, rowIdx)
			}
		}

		id := make([]interface{}, 1)
		id[0] = row[p.cfg.FactIDColumn]
		idCanon := canonical(id)

		keys := make([]interface{}, len(p.cfg.FactKeyColumns))
		for i, col := range p.cfg.FactKeyColumns {
			keys[i] = row[col]
		}
		keyCanon := canonical(keys)

		if isDelete {
			plan[idCanon] = &factMutation{delete: true}
			if existing, ok := p.facts[idCanon]; ok {
				affected[existing.keyCanon] = struct{}{}
			}
			continue
		}

		measures := make([]interface{}, len(measureNames))
		for i, col := range measureNames {
			measures[i] = row[col]
		}
		plan[idCanon] = &factMutation{
			record: &factRecord{
				id:       id,
				idCanon:  idCanon,
				key:      keys,
				keyCanon: keyCanon,
				time:     row[p.cfg.FactTimeColumn],
				measures: measures,
			},
		}
		affected[keyCanon] = struct{}{}
		// A re-upload under an existing id can move the order to another
		// dimension key; the old key must be recomputed as well.
		if existing, ok := p.facts[idCanon]; ok {
			affected[existing.keyCanon] = struct{}{}
		}
	}

	if !p.factLocked {
		p.idCol = schemaCol{name: p.cfg.FactIDColumn, t: idType}
		p.factKeyCols = make([]schemaCol, len(p.cfg.FactKeyColumns))
		for i, col := range p.cfg.FactKeyColumns {
			p.factKeyCols[i] = schemaCol{name: col, t: factKeyTypes[i]}
		}
		p.factTimeCol = schemaCol{name: p.cfg.FactTimeColumn, t: factTimeType}
		p.measureCols = make([]schemaCol, len(measureNames))
		for i, n := range measureNames {
			p.measureCols[i] = schemaCol{name: n, t: measureTypes[i]}
		}
		p.factOpColumn = opCol
		p.factTimeType = factTimeType
		p.factKeyTypes = factKeyTypes
		p.factIDType = idType
		p.factLocked = true
	}
	return plan, affected, nil
}

func checkSchemaLock(kind string, locked []schemaCol, names []string, types []series.Type) error {
	if len(locked) != len(names) {
		return fmt.Errorf("scd: %s schema changed: expected %d columns, got %d", kind, len(locked), len(names))
	}
	for i := range locked {
		if locked[i].name != names[i] || locked[i].t != types[i] {
			return fmt.Errorf("scd: %s schema changed: expected %q (%s), got %q (%s)",
				kind, locked[i].name, locked[i].t, names[i], types[i])
		}
	}
	return nil
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// Apply validates and applies one batch containing dimension events and/or
// fact rows. Either frame may be the zero-value dataframe.DataFrame when that
// side has no rows in the batch. The whole batch takes effect atomically: if
// any row is invalid, no state is modified and the returned error explains the
// first problem found.
func (p *Processor) Apply(dim, fact dataframe.DataFrame) error {
	// parse* locks the input schema on success; remember whether each side was
	// already locked so a failure later in the batch rolls the lock back.
	dimLockedBefore, factLockedBefore := p.dimLocked, p.factLocked
	affected := map[string]struct{}{}

	var dimPlan map[string]map[interface{}]*dimMutation
	if dim.Nrow() > 0 {
		plan, keys, err := p.parseDimensions(dim)
		if err != nil {
			p.rollbackSchema(dimLockedBefore, factLockedBefore)
			return err
		}
		dimPlan = plan
		for k := range keys {
			affected[k] = struct{}{}
		}
	}

	var factPlan map[string]*factMutation
	if fact.Nrow() > 0 {
		if !p.factConfigured() {
			p.rollbackSchema(dimLockedBefore, factLockedBefore)
			return fmt.Errorf("scd: processor was not configured for fact frames")
		}
		plan, keys, err := p.parseFacts(fact)
		if err != nil {
			p.rollbackSchema(dimLockedBefore, factLockedBefore)
			return err
		}
		factPlan = plan
		for k := range keys {
			affected[k] = struct{}{}
		}
	}

	// Both frames parsed cleanly: commit the dimension mutations.
	for keyCanon, byTime := range dimPlan {
		bucket, ok := p.dimEvents[keyCanon]
		if !ok {
			bucket = map[interface{}]*dimEvent{}
			p.dimEvents[keyCanon] = bucket
		}
		for t, mut := range byTime {
			if mut.delete {
				delete(bucket, t)
			} else {
				bucket[t] = mut.event
			}
		}
	}

	// Commit the fact mutations.
	for idCanon, mut := range factPlan {
		if mut.delete {
			delete(p.facts, idCanon)
			continue
		}
		p.facts[idCanon] = mut.record
	}

	p.rebuild(affected)
	return nil
}

// rollbackSchema restores schema-lock state when a batch fails after one side
// had already locked its schema; previously locked schemas never change.
func (p *Processor) rollbackSchema(dimLockedBefore, factLockedBefore bool) {
	if !dimLockedBefore && p.dimLocked {
		p.keyCols, p.attrCols = nil, nil
		p.timeCol = schemaCol{}
		p.dimOpColumn, p.dimLocked = "", false
	}
	if !factLockedBefore && p.factLocked {
		p.idCol = schemaCol{}
		p.factKeyCols, p.measureCols, p.factKeyTypes = nil, nil, nil
		p.factTimeCol = schemaCol{}
		p.factOpColumn, p.factTimeType, p.factIDType, p.factLocked = "", "", "", false
	}
}

func (p *Processor) factConfigured() bool {
	return p.cfg.FactIDColumn != "" && p.cfg.FactTimeColumn != "" && len(p.cfg.FactKeyColumns) > 0
}

// ApplyDimensions is a convenience wrapper for dimension-only batches.
func (p *Processor) ApplyDimensions(dim dataframe.DataFrame) error {
	return p.Apply(dim, dataframe.DataFrame{})
}

// ApplyFacts is a convenience wrapper for fact-only batches.
func (p *Processor) ApplyFacts(fact dataframe.DataFrame) error {
	return p.Apply(dataframe.DataFrame{}, fact)
}

// rebuild recomputes the SCD2 versions of the affected keys and re-resolves
// every fact referencing those keys.
func (p *Processor) rebuild(affected map[string]struct{}) {
	// Sort affected keys so freshly allocated version keys are deterministic.
	keyCanons := make([]string, 0, len(affected))
	for keyCanon := range affected {
		keyCanons = append(keyCanons, keyCanon)
	}
	sort.Strings(keyCanons)
	for _, keyCanon := range keyCanons {
		versions := p.buildVersions(keyCanon)
		p.versions[keyCanon] = versions

		// Retire identities that no longer correspond to a version start.
		alive := make(map[interface{}]bool, len(versions))
		for _, v := range versions {
			alive[v.start] = true
		}
		for id := range p.identity {
			if id.key == keyCanon && !alive[id.time] {
				delete(p.identity, id)
			}
		}
	}

	for _, f := range p.facts {
		if _, ok := affected[f.keyCanon]; ok {
			p.resolve(f)
		}
	}
}

// buildVersions materializes the half-open versions of one business key.
// Adjacent events carrying identical attributes are merged into one version,
// and version keys are assigned from the stable identity registry.
func (p *Processor) buildVersions(keyCanon string) []version {
	bucket := p.dimEvents[keyCanon]
	if len(bucket) == 0 {
		return nil
	}
	times := make([]interface{}, 0, len(bucket))
	for t := range bucket {
		times = append(times, t)
	}
	sort.Slice(times, func(i, j int) bool { return timeCmp(times[i], times[j]) < 0 })

	type segment struct {
		start interface{}
		attrs attrSet
	}
	var segments []segment
	for _, t := range times {
		ev := bucket[t]
		if len(segments) == 0 || !segments[len(segments)-1].attrs.equal(ev.attrs) {
			segments = append(segments, segment{start: t, attrs: ev.attrs})
		}
		// Identical adjacent attributes: keep the earlier start, dropping the
		// boundary of the new event.
	}

	versions := make([]version, len(segments))
	for i, seg := range segments {
		id := identityKey{key: keyCanon, time: seg.start}
		key, ok := p.identity[id]
		if !ok {
			p.counter++
			key = p.counter
			p.identity[id] = key
		}
		v := version{start: seg.start, attrs: seg.attrs, key: key}
		if i+1 < len(segments) {
			v.end = segments[i+1].start
		}
		versions[i] = v
	}
	return versions
}

// resolve associates a fact with the version in effect at its business time.
// A fact stays unmatched when its key has no version or its time precedes the
// first version start; intervals are half-open, so an order at an end time is
// matched by the next version.
func (p *Processor) resolve(f *factRecord) {
	versions := p.versions[f.keyCanon]
	if len(versions) == 0 || timeCmp(f.time, versions[0].start) < 0 {
		f.matched = false
		f.vk = 0
		f.attrs = attrSet{}
		return
	}
	idx := 0
	for i, v := range versions {
		if timeCmp(f.time, v.start) < 0 {
			break
		}
		idx = i
	}
	selected := versions[idx]
	f.matched = true
	f.vk = selected.key
	f.attrs = attrSet{values: append([]interface{}(nil), selected.attrs.values...)}
}

// Dimensions exports the current SCD2 dimension table. Column order is the
// business key columns, version_key, start_time, end_time (NA for the open
// version) and the tracked attribute columns. Rows are ordered by business key
// and start time. The returned frame is a regular gota DataFrame that can be
// written, joined or otherwise processed natively.
func (p *Processor) Dimensions() dataframe.DataFrame {
	if !p.dimLocked {
		return dataframe.DataFrame{}
	}
	keyCanons := make([]string, 0, len(p.versions))
	for keyCanon := range p.versions {
		keyCanons = append(keyCanons, keyCanon)
	}
	sort.Strings(keyCanons)

	nrows := 0
	for _, keyCanon := range keyCanons {
		nrows += len(p.versions[keyCanon])
	}

	keyCols := len(p.keyCols)
	attrCols := len(p.attrCols)
	keysOut := make([][]interface{}, keyCols)
	for i := range keysOut {
		keysOut[i] = make([]interface{}, 0, nrows)
	}
	vk := make([]int, 0, nrows)
	start := make([]interface{}, 0, nrows)
	end := make([]interface{}, 0, nrows)
	attrsOut := make([][]interface{}, attrCols)
	for i := range attrsOut {
		attrsOut[i] = make([]interface{}, 0, nrows)
	}

	// Resolve the original key values per canonical key.
	keyValues := map[string][]interface{}{}
	for keyCanon, bucket := range p.dimEvents {
		for _, ev := range bucket {
			keyValues[keyCanon] = ev.key
			break
		}
	}

	for _, keyCanon := range keyCanons {
		for _, v := range p.versions[keyCanon] {
			keys := keyValues[keyCanon]
			for i := 0; i < keyCols; i++ {
				keysOut[i] = append(keysOut[i], keys[i])
			}
			vk = append(vk, v.key)
			start = append(start, v.start)
			end = append(end, v.end)
			for i := 0; i < attrCols; i++ {
				attrsOut[i] = append(attrsOut[i], v.attrs.values[i])
			}
		}
	}

	col := make([]series.Series, 0, keyCols+3+attrCols)
	for i, sc := range p.keyCols {
		col = append(col, series.New(keysOut[i], sc.t, sc.name))
	}
	col = append(col,
		series.New(vk, series.Int, VersionKeyColumn),
		series.New(start, p.timeCol.t, StartTimeColumn),
		series.New(end, p.timeCol.t, EndTimeColumn),
	)
	for i, sc := range p.attrCols {
		col = append(col, series.New(attrsOut[i], sc.t, sc.name))
	}
	return dataframe.New(col...)
}

// Facts exports the current fact table with its maintained dimension
// association. Column order is the fact id, the referenced business key
// columns, the fact time, version_key (NA when unmatched), status, the
// dimension attributes (NA when unmatched) and the measures. Rows are ordered
// by fact id. Like Dimensions, the result is a native gota DataFrame.
func (p *Processor) Facts() dataframe.DataFrame {
	if !p.factLocked {
		return dataframe.DataFrame{}
	}
	idCanons := make([]string, 0, len(p.facts))
	for idCanon := range p.facts {
		idCanons = append(idCanons, idCanon)
	}
	sort.Strings(idCanons)

	nrows := len(idCanons)
	idOut := make([]interface{}, 0, nrows)
	keyOut := make([][]interface{}, len(p.factKeyCols))
	for i := range keyOut {
		keyOut[i] = make([]interface{}, 0, nrows)
	}
	timeOut := make([]interface{}, 0, nrows)
	vkOut := make([]interface{}, 0, nrows)
	statusOut := make([]string, 0, nrows)
	attrsOut := make([][]interface{}, len(p.attrCols))
	for i := range attrsOut {
		attrsOut[i] = make([]interface{}, 0, nrows)
	}
	measureOut := make([][]interface{}, len(p.measureCols))
	for i := range measureOut {
		measureOut[i] = make([]interface{}, 0, nrows)
	}

	for _, idCanon := range idCanons {
		f := p.facts[idCanon]
		idOut = append(idOut, f.id[0])
		for i, v := range f.key {
			keyOut[i] = append(keyOut[i], v)
		}
		timeOut = append(timeOut, f.time)
		if f.matched {
			vkOut = append(vkOut, f.vk)
			statusOut = append(statusOut, StatusMatched)
			for i := range p.attrCols {
				attrsOut[i] = append(attrsOut[i], f.attrs.values[i])
			}
		} else {
			vkOut = append(vkOut, nil)
			statusOut = append(statusOut, StatusUnmatched)
			for i := range p.attrCols {
				attrsOut[i] = append(attrsOut[i], nil)
			}
		}
		for i, v := range f.measures {
			measureOut[i] = append(measureOut[i], v)
		}
	}

	col := make([]series.Series, 0)
	col = append(col, series.New(idOut, p.idCol.t, p.idCol.name))
	for i, sc := range p.factKeyCols {
		col = append(col, series.New(keyOut[i], sc.t, sc.name))
	}
	col = append(col, series.New(timeOut, p.factTimeCol.t, p.factTimeCol.name))
	col = append(col,
		series.New(vkOut, series.Int, VersionKeyColumn),
		series.New(statusOut, series.String, StatusColumn),
	)
	for i, sc := range p.attrCols {
		col = append(col, series.New(attrsOut[i], sc.t, sc.name))
	}
	for i, sc := range p.measureCols {
		col = append(col, series.New(measureOut[i], sc.t, sc.name))
	}
	return dataframe.New(col...)
}

// Reset clears all maintained events, versions and facts while keeping the
// processor configuration.
func (p *Processor) Reset() {
	p.dimEvents = map[string]map[interface{}]*dimEvent{}
	p.facts = map[string]*factRecord{}
	p.versions = map[string][]version{}
	p.identity = map[identityKey]int{}
	p.counter = 0
	p.keyCols, p.attrCols = nil, nil
	p.timeCol = schemaCol{}
	p.dimLocked = false
	p.dimOpColumn = ""
	p.idCol = schemaCol{}
	p.factKeyCols, p.measureCols, p.factKeyTypes = nil, nil, nil
	p.factTimeCol = schemaCol{}
	p.factLocked = false
	p.factOpColumn = ""
	p.factTimeType, p.factIDType = "", ""
}
