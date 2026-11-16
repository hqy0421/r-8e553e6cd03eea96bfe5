# scd — slowly changing dimension (type 2) support

The `scd` package maintains SCD2 dimension history and an order fact table on
top of the gota `dataframe`/`series` primitives. It exists because the gota
join operators only perform equality comparisons: they cannot associate a
historical fact row with the dimension version that was in effect at the
order's business time, so the temporal association is maintained here
directly.

## Concepts

Dimension input frames describe attribute events:

- `DimKeyColumns` — the business key (one or more columns).
- `DimTimeColumn` — the effective time of the event (`int`, `float` or
  lexicographically ordered `string`, e.g. `YYYY-MM-DD`).
- `DimOpColumn` (optional) — `upsert`/`delete` markers. Values are
  case-insensitive and configurable through `Config.UpsertValues` /
  `Config.DeleteValues`. A frame without the operation column is treated as
  all upserts.
- every other column is a tracked attribute.

Fact input frames (optional) describe orders:

- `FactIDColumn` — order identifier; re-uploading an id replaces the previous
  order row.
- `FactKeyColumns` — business key columns referencing the dimension, in the
  same order as `DimKeyColumns`.
- `FactTimeColumn` — order business time; must share the dimension time type.
- `FactOpColumn` (optional) — order-level upsert/delete.
- every other column is a measure carried through to the export.

Events are organized into half-open `[start_time, end_time)` versions.
Adjacent events with identical attributes (NA compares equal to NA) are merged
into one version starting at the earlier effective time. The open (current)
version has an NA `end_time`.

`version_key` is bound to the pair *(business key, start time)*. As long as
that identity survives a correction or revocation, the key is stable; new
identities receive fresh keys and removed identities are retired.

## Maintenance semantics

- **Late correction** — an event at an earlier effective time is inserted into
  history, intervals are rebuilt and facts whose association changed are
  re-pointed.
- **Revocation** — a `delete` event removes the event at that effective time;
  neighboring versions merge when the attributes become adjacent-equal and
  affected facts are re-resolved.
- **Unmatched facts** — orders whose time precedes the first version, or whose
  business key has no dimension, keep `status = "unmatched"`, an NA
  `version_key` and NA dimension attributes. They are retried automatically on
  every later batch.
- **Atomic batches** — a dimension frame and a fact frame passed to one
  `Apply` call take effect together, so facts can match dimensions arriving in
  the same batch. All validation runs before any mutation: an invalid batch
  changes nothing.
- **Incremental recomputation** — only the business keys touched by a batch are
  rebuilt and re-resolved.

## Example

```go
processor, err := scd.NewProcessor(scd.Config{
    DimKeyColumns:  []string{"cust_id"},
    DimTimeColumn:  "eff",
    FactIDColumn:   "order_id",
    FactKeyColumns: []string{"cust_id"},
    FactTimeColumn: "ts",
})
if err != nil {
    log.Fatal(err)
}

// Dimension history and orders can arrive together or in separate batches.
if err := processor.Apply(dimensions, orders); err != nil {
    log.Fatal(err)
}

// Native gota DataFrames, ready for WriteCSV, joins, aggregation, etc.
dimTable := processor.Dimensions()
factTable := processor.Facts()

// version_key is an ordinary equality key for the native joins.
enriched := factTable.InnerJoin(
    dimTable.Select([]string{"version_key", "start_time", "end_time"}),
    "version_key",
)
```

## Exported schemas

`Dimensions()`: business key columns, `version_key`, `start_time`,
`end_time`, attribute columns — ordered by business key and start time.

`Facts()`: fact id, business key columns, fact time, `version_key`, `status`,
dimension attribute columns, measure columns — ordered by fact id.
