use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use floe_cdc::{CdcRowDelta, CdcTableDeltas};
use floe_cdc_core::{CdcRow, CdcTableId};
use floe_core::RowValue;
use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};
use floe_executor::SourceRowDecoder;
use floe_node_core::cdc_delta_encoder::{
    CdcArrowDeltaBatch, encode_cdc_arrow_delta_batch, encode_cdc_table_deltas,
    encode_cdc_table_deltas_rowwise,
};

fn source_definition() -> SourceDefinition {
    SourceDefinition::new(
        "nexmark_bid",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("channel", SourceDataType::Utf8, true),
            SourceColumn::new_nullable("url", SourceDataType::Utf8, true),
            SourceColumn::new_nullable("date_time", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, true),
        ],
    )
    .expect("source definition")
}

fn row(idx: usize) -> CdcRow {
    CdcRow::new([
        Some(RowValue::Int64(i64::try_from(idx).expect("idx fits i64"))),
        Some(RowValue::Int64(
            i64::try_from(idx % 10_000).expect("bidder fits i64"),
        )),
        Some(RowValue::Int64(
            i64::try_from(100 + (idx % 500)).expect("price fits i64"),
        )),
        Some(RowValue::Utf8("web".to_string())),
        Some(RowValue::Utf8("http://example.com".to_string())),
        Some(RowValue::TimestampMillis(
            1_700_000_000_000_i64 + i64::try_from(idx).expect("idx fits i64"),
        )),
        Some(RowValue::Utf8("cdc-bench".to_string())),
    ])
    .expect("row")
}

fn deltas(batch_size: usize) -> CdcTableDeltas {
    let changes = (0..batch_size)
        .map(|idx| {
            if idx % 8 == 0 {
                CdcRowDelta::delete(row(idx))
            } else {
                CdcRowDelta::insert(row(idx))
            }
        })
        .collect();
    CdcTableDeltas::new(CdcTableId::new("nexmark_bid").expect("table id"), changes)
}

fn bench_postgres_cdc_arrow_encoder(c: &mut Criterion) {
    let definition = source_definition();
    let decoder = SourceRowDecoder::new(definition.clone());
    let mut group = c.benchmark_group("postgres_cdc_arrow_encoder");
    for batch_size in [1_024_usize, 16_384] {
        let deltas = deltas(batch_size);
        let arrow_batch =
            CdcArrowDeltaBatch::from_table_deltas(&definition, &deltas).expect("arrow batch");
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_function(BenchmarkId::new("rowwise_encode", batch_size), |b| {
            b.iter(|| {
                let encoded =
                    encode_cdc_table_deltas_rowwise(black_box(&decoder), black_box(&deltas))
                        .expect("rowwise encode");
                black_box(encoded);
            });
        });

        group.bench_function(
            BenchmarkId::new("arrow_build_and_encode", batch_size),
            |b| {
                b.iter(|| {
                    let encoded = encode_cdc_table_deltas(black_box(&decoder), black_box(&deltas))
                        .expect("arrow encode");
                    black_box(encoded);
                });
            },
        );

        group.bench_function(BenchmarkId::new("arrow_encode_only", batch_size), |b| {
            b.iter(|| {
                let encoded =
                    encode_cdc_arrow_delta_batch(black_box(&decoder), black_box(&arrow_batch))
                        .expect("arrow encode only");
                black_box(encoded);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_postgres_cdc_arrow_encoder);
criterion_main!(benches);
