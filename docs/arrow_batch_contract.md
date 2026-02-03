# Arrow Batch Contract for DBSP Deltas

This document defines the Arrow `RecordBatch` layout used for DBSP delta
batches in Floe's vectorized execution path.

## RecordBatch Layout

A delta batch is a standard Arrow `RecordBatch` with the following columns in
order:

1. **Row columns**: the columns from the DBSP row schema, in schema order.
2. **Optional key column**: `__key` (`Binary`, non-null). This is reserved for
   keyed consolidation and is currently optional.
3. **Weight column**: `__weight` (`Int64`, non-null). This is required for all
   delta batches and represents the integer multiplicity of the row.

The weight column is always last; if `__key` is present, it appears immediately
before `__weight`.

### Reserved Column Names

Row schemas must not contain the names `__weight` or `__key`. These names are
reserved for delta metadata.

## Weight Semantics

- `__weight` is an `i64` delta weight.
- Positive values insert / add weight.
- Negative values delete / subtract weight.
- Rows with `__weight == 0` are semantically no-ops and should be dropped
  during consolidation.

## rkyv -> Arrow Encoding/Decoding Rules

Floe uses rkyv for row serialization (`dbsp::storage::encoding::{encode, decode}`)
when producing and consuming vectorized batches.

### Supported Types

| DBSP scalar type | Arrow type | Nullable |
| --- | --- | --- |
| `Int64` | `Int64` | schema-controlled |
| `Utf8` | `Utf8` | schema-controlled |
| `Bool` | `Boolean` | schema-controlled |
| `TimestampMillis` | `Timestamp(Millisecond, None)` | schema-controlled |

### Encoding Rules

- Each row is rkyv-decoded into a typed row struct.
- The Arrow arrays are built column-wise from decoded values.
- Null handling follows the row schema: a null is only valid if the field is
  declared nullable.
- `__weight` is always written as a non-null `Int64`.

### Optional `__key`

When present, `__key` is a binary encoding of the row's primary key, using
`dbsp-circuit/src/circuit/encoding.rs` (`encode_composite_key`). The encoding
is stable and little-endian for fixed-width scalars; strings are length-prefixed.

## Helper APIs

The canonical schema helpers live in `dbsp-circuit/src/circuit/arrow_batch.rs`:

- `delta_arrow_schema(row_schema, include_key)`
- `delta_arrow_fields(row_schema, include_key)`
- `WEIGHT_COLUMN_NAME` / `KEY_COLUMN_NAME`
