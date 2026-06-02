use super::*;

const TRANSIENT_COUNT_AGGREGATE_GROUP_TAG: u8 = 1;
const TRANSIENT_COUNT_AGGREGATE_DISTINCT_TAG: u8 = 2;
const TRANSIENT_INCREMENTAL_AGGREGATE_GROUP_TAG: u8 = 11;
const TRANSIENT_INCREMENTAL_AGGREGATE_DISTINCT_TAG: u8 = 12;
const TRANSIENT_INCREMENTAL_AGGREGATE_INPUT_TAG: u8 = 13;
const AGGREGATE_VALUE_NULL_INT64_TAG: u8 = 1;
const AGGREGATE_VALUE_NULL_TIMESTAMP_MILLIS_TAG: u8 = 2;
const AGGREGATE_VALUE_NULL_UTF8_TAG: u8 = 3;
const AGGREGATE_VALUE_INT64_TAG: u8 = 4;
const AGGREGATE_VALUE_TIMESTAMP_MILLIS_TAG: u8 = 5;
const AGGREGATE_VALUE_UTF8_TAG: u8 = 6;
const AGGREGATE_VALUE_NULL_DATE_DAYS_TAG: u8 = 7;
const AGGREGATE_VALUE_NULL_DECIMAL128_TAG: u8 = 8;
const AGGREGATE_VALUE_DATE_DAYS_TAG: u8 = 9;
const AGGREGATE_VALUE_DECIMAL128_TAG: u8 = 10;
const INCREMENTAL_AGGREGATE_SLOT_COUNT_TAG: u8 = 1;
const INCREMENTAL_AGGREGATE_SLOT_COUNT_DISTINCT_TAG: u8 = 2;
const INCREMENTAL_AGGREGATE_SLOT_SUM_TAG: u8 = 3;
const INCREMENTAL_AGGREGATE_SLOT_AVG_TAG: u8 = 4;
const INCREMENTAL_AGGREGATE_SLOT_MIN_TAG: u8 = 5;
const INCREMENTAL_AGGREGATE_SLOT_MAX_TAG: u8 = 6;
const INCREMENTAL_AGGREGATE_SLOT_DECIMAL_SUM_TAG: u8 = 7;

pub(super) fn encode_transient_count_aggregate_snapshot(
    snapshot: dbsp::TransientCountAggregateSnapshot<Vec<u8>, Vec<u8>>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    let mut rows = Vec::with_capacity(snapshot.grouped.len() + snapshot.distinct.len());
    for group in snapshot.grouped {
        if group.total_rows == 0 {
            continue;
        }
        let mut row = Vec::new();
        row.push(TRANSIENT_COUNT_AGGREGATE_GROUP_TAG);
        write_len_prefixed_bytes(&mut row, &group.key)?;
        row.extend_from_slice(&group.total_rows.to_le_bytes());
        let count_len = u32::try_from(group.counts.len())
            .map_err(|_| anyhow!("too many transient count aggregate slots"))?;
        row.extend_from_slice(&count_len.to_le_bytes());
        for count in group.counts {
            row.extend_from_slice(&count.to_le_bytes());
        }
        rows.push((row, 1));
    }
    for distinct in snapshot.distinct {
        if distinct.weight == 0 {
            continue;
        }
        let mut row = Vec::new();
        row.push(TRANSIENT_COUNT_AGGREGATE_DISTINCT_TAG);
        write_len_prefixed_bytes(&mut row, &distinct.group_key)?;
        row.extend_from_slice(&distinct.slot.to_le_bytes());
        write_len_prefixed_bytes(&mut row, &distinct.value)?;
        rows.push((row, distinct.weight));
    }
    Ok(rows)
}

pub(super) fn decode_transient_count_aggregate_snapshot(
    rows: Vec<(Vec<u8>, i64)>,
) -> Result<dbsp::TransientCountAggregateSnapshot<Vec<u8>, Vec<u8>>> {
    let mut snapshot = dbsp::TransientCountAggregateSnapshot::default();
    for (row, weight) in rows {
        if row.is_empty() || weight == 0 {
            continue;
        }
        let mut cursor = 1usize;
        match row[0] {
            TRANSIENT_COUNT_AGGREGATE_GROUP_TAG => {
                let key = read_len_prefixed_bytes(&row, &mut cursor)?;
                let total_rows = read_i64_le(&row, &mut cursor)?;
                let count_len = read_u32_le(&row, &mut cursor)? as usize;
                let mut counts = Vec::with_capacity(count_len);
                for _ in 0..count_len {
                    counts.push(read_i64_le(&row, &mut cursor)?);
                }
                if cursor != row.len() {
                    bail!("trailing bytes in transient count aggregate group state row");
                }
                snapshot
                    .grouped
                    .push(dbsp::TransientCountAggregateGroupedState {
                        key,
                        total_rows,
                        counts,
                    });
            }
            TRANSIENT_COUNT_AGGREGATE_DISTINCT_TAG => {
                let group_key = read_len_prefixed_bytes(&row, &mut cursor)?;
                let slot = read_u32_le(&row, &mut cursor)?;
                let value = read_len_prefixed_bytes(&row, &mut cursor)?;
                if cursor != row.len() {
                    bail!("trailing bytes in transient count aggregate distinct state row");
                }
                snapshot
                    .distinct
                    .push(dbsp::TransientCountAggregateDistinctWeight {
                        group_key,
                        slot,
                        value,
                        weight,
                    });
            }
            other => bail!("unknown transient count aggregate state row tag {other}"),
        }
    }
    Ok(snapshot)
}

pub(super) fn encode_transient_incremental_aggregate_snapshot(
    snapshot: dbsp::TransientIncrementalAggregateSnapshot<Vec<u8>, Vec<u8>>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    let mut rows =
        Vec::with_capacity(snapshot.grouped.len() + snapshot.distinct.len() + snapshot.input.len());
    for group in snapshot.grouped {
        if group.total_rows == 0 {
            continue;
        }
        let mut row = Vec::new();
        row.push(TRANSIENT_INCREMENTAL_AGGREGATE_GROUP_TAG);
        write_len_prefixed_bytes(&mut row, &group.key)?;
        row.extend_from_slice(&group.total_rows.to_le_bytes());
        let slot_len = u32::try_from(group.slots.len())
            .map_err(|_| anyhow!("too many transient incremental aggregate slots"))?;
        row.extend_from_slice(&slot_len.to_le_bytes());
        for slot in group.slots {
            encode_incremental_aggregate_slot_state(&mut row, slot)?;
        }
        rows.push((row, 1));
    }
    for distinct in snapshot.distinct {
        if distinct.weight == 0 {
            continue;
        }
        let mut row = Vec::new();
        row.push(TRANSIENT_INCREMENTAL_AGGREGATE_DISTINCT_TAG);
        write_len_prefixed_bytes(&mut row, &distinct.group_key)?;
        row.extend_from_slice(&distinct.slot.to_le_bytes());
        encode_aggregate_value(&mut row, distinct.value)?;
        rows.push((row, distinct.weight));
    }
    for input in snapshot.input {
        if input.weight == 0 {
            continue;
        }
        let mut row = Vec::new();
        row.push(TRANSIENT_INCREMENTAL_AGGREGATE_INPUT_TAG);
        write_len_prefixed_bytes(&mut row, &input.group_key)?;
        write_len_prefixed_bytes(&mut row, &input.value)?;
        rows.push((row, input.weight));
    }
    Ok(rows)
}

pub(super) fn decode_transient_incremental_aggregate_snapshot(
    rows: Vec<(Vec<u8>, i64)>,
) -> Result<dbsp::TransientIncrementalAggregateSnapshot<Vec<u8>, Vec<u8>>> {
    let mut snapshot = dbsp::TransientIncrementalAggregateSnapshot::default();
    for (row, weight) in rows {
        if row.is_empty() || weight == 0 {
            continue;
        }
        let mut cursor = 1usize;
        match row[0] {
            TRANSIENT_INCREMENTAL_AGGREGATE_GROUP_TAG => {
                let key = read_len_prefixed_bytes(&row, &mut cursor)?;
                let total_rows = read_i64_le(&row, &mut cursor)?;
                let slot_len = read_u32_le(&row, &mut cursor)? as usize;
                let mut slots = Vec::with_capacity(slot_len);
                for _ in 0..slot_len {
                    slots.push(decode_incremental_aggregate_slot_state(&row, &mut cursor)?);
                }
                if cursor != row.len() {
                    bail!("trailing bytes in transient incremental aggregate group state row");
                }
                snapshot
                    .grouped
                    .push(dbsp::TransientIncrementalAggregateGroupedState {
                        key,
                        total_rows,
                        slots,
                    });
            }
            TRANSIENT_INCREMENTAL_AGGREGATE_DISTINCT_TAG => {
                let group_key = read_len_prefixed_bytes(&row, &mut cursor)?;
                let slot = read_u32_le(&row, &mut cursor)?;
                let value = decode_aggregate_value(&row, &mut cursor)?;
                if cursor != row.len() {
                    bail!("trailing bytes in transient incremental aggregate distinct state row");
                }
                snapshot
                    .distinct
                    .push(dbsp::TransientIncrementalAggregateDistinctWeight {
                        group_key,
                        slot,
                        value,
                        weight,
                    });
            }
            TRANSIENT_INCREMENTAL_AGGREGATE_INPUT_TAG => {
                let group_key = read_len_prefixed_bytes(&row, &mut cursor)?;
                let value = read_len_prefixed_bytes(&row, &mut cursor)?;
                if cursor != row.len() {
                    bail!("trailing bytes in transient incremental aggregate input state row");
                }
                snapshot
                    .input
                    .push(dbsp::TransientIncrementalAggregateInputWeight {
                        group_key,
                        value,
                        weight,
                    });
            }
            other => bail!("unknown transient incremental aggregate state row tag {other}"),
        }
    }
    Ok(snapshot)
}

pub(super) fn encode_transient_window_incremental_aggregate_snapshot(
    snapshot: dbsp::TransientIncrementalAggregateSnapshot<Vec<u8>, (Vec<u8>, Vec<u8>)>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    let input = snapshot
        .input
        .into_iter()
        .map(|entry| {
            let value =
                encode_transient_window_aggregate_input_pair(&entry.value.0, &entry.value.1)?;
            Ok(dbsp::TransientIncrementalAggregateInputWeight {
                group_key: entry.group_key,
                value,
                weight: entry.weight,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    encode_transient_incremental_aggregate_snapshot(dbsp::TransientIncrementalAggregateSnapshot {
        grouped: snapshot.grouped,
        distinct: snapshot.distinct,
        input,
    })
}

pub(super) fn decode_transient_window_incremental_aggregate_snapshot(
    rows: Vec<(Vec<u8>, i64)>,
) -> Result<dbsp::TransientIncrementalAggregateSnapshot<Vec<u8>, (Vec<u8>, Vec<u8>)>> {
    let snapshot = decode_transient_incremental_aggregate_snapshot(rows)?;
    let input = snapshot
        .input
        .into_iter()
        .map(|entry| {
            let value = decode_transient_window_aggregate_input_pair(&entry.value)?;
            Ok(dbsp::TransientIncrementalAggregateInputWeight {
                group_key: entry.group_key,
                value,
                weight: entry.weight,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(dbsp::TransientIncrementalAggregateSnapshot {
        grouped: snapshot.grouped,
        distinct: snapshot.distinct,
        input,
    })
}

pub(super) fn encode_incremental_aggregate_slot_state(
    dst: &mut Vec<u8>,
    slot: dbsp::IncrementalAggregateSlotState,
) -> Result<()> {
    match slot {
        dbsp::IncrementalAggregateSlotState::Count { count } => {
            dst.push(INCREMENTAL_AGGREGATE_SLOT_COUNT_TAG);
            dst.extend_from_slice(&count.to_le_bytes());
        }
        dbsp::IncrementalAggregateSlotState::CountDistinct { count } => {
            dst.push(INCREMENTAL_AGGREGATE_SLOT_COUNT_DISTINCT_TAG);
            dst.extend_from_slice(&count.to_le_bytes());
        }
        dbsp::IncrementalAggregateSlotState::Sum {
            sum,
            non_null_count,
        } => {
            dst.push(INCREMENTAL_AGGREGATE_SLOT_SUM_TAG);
            dst.extend_from_slice(&sum.to_le_bytes());
            dst.extend_from_slice(&non_null_count.to_le_bytes());
        }
        dbsp::IncrementalAggregateSlotState::DecimalSum {
            sum,
            non_null_count,
        } => {
            dst.push(INCREMENTAL_AGGREGATE_SLOT_DECIMAL_SUM_TAG);
            dst.extend_from_slice(&sum.to_le_bytes());
            dst.extend_from_slice(&non_null_count.to_le_bytes());
        }
        dbsp::IncrementalAggregateSlotState::Avg { sum, count } => {
            dst.push(INCREMENTAL_AGGREGATE_SLOT_AVG_TAG);
            dst.extend_from_slice(&sum.to_le_bytes());
            dst.extend_from_slice(&count.to_le_bytes());
        }
        dbsp::IncrementalAggregateSlotState::Min { current } => {
            dst.push(INCREMENTAL_AGGREGATE_SLOT_MIN_TAG);
            encode_optional_aggregate_value(dst, current)?;
        }
        dbsp::IncrementalAggregateSlotState::Max { current } => {
            dst.push(INCREMENTAL_AGGREGATE_SLOT_MAX_TAG);
            encode_optional_aggregate_value(dst, current)?;
        }
    }
    Ok(())
}

pub(super) fn decode_incremental_aggregate_slot_state(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<dbsp::IncrementalAggregateSlotState> {
    let tag = read_u8(bytes, cursor)?;
    match tag {
        INCREMENTAL_AGGREGATE_SLOT_COUNT_TAG => Ok(dbsp::IncrementalAggregateSlotState::Count {
            count: read_i64_le(bytes, cursor)?,
        }),
        INCREMENTAL_AGGREGATE_SLOT_COUNT_DISTINCT_TAG => {
            Ok(dbsp::IncrementalAggregateSlotState::CountDistinct {
                count: read_i64_le(bytes, cursor)?,
            })
        }
        INCREMENTAL_AGGREGATE_SLOT_SUM_TAG => Ok(dbsp::IncrementalAggregateSlotState::Sum {
            sum: read_i64_le(bytes, cursor)?,
            non_null_count: read_i64_le(bytes, cursor)?,
        }),
        INCREMENTAL_AGGREGATE_SLOT_DECIMAL_SUM_TAG => {
            Ok(dbsp::IncrementalAggregateSlotState::DecimalSum {
                sum: read_i128_le(bytes, cursor)?,
                non_null_count: read_i64_le(bytes, cursor)?,
            })
        }
        INCREMENTAL_AGGREGATE_SLOT_AVG_TAG => Ok(dbsp::IncrementalAggregateSlotState::Avg {
            sum: read_i64_le(bytes, cursor)?,
            count: read_i64_le(bytes, cursor)?,
        }),
        INCREMENTAL_AGGREGATE_SLOT_MIN_TAG => Ok(dbsp::IncrementalAggregateSlotState::Min {
            current: decode_optional_aggregate_value(bytes, cursor)?,
        }),
        INCREMENTAL_AGGREGATE_SLOT_MAX_TAG => Ok(dbsp::IncrementalAggregateSlotState::Max {
            current: decode_optional_aggregate_value(bytes, cursor)?,
        }),
        other => bail!("unknown incremental aggregate slot state tag {other}"),
    }
}

pub(super) fn encode_optional_aggregate_value(
    dst: &mut Vec<u8>,
    value: Option<dbsp::AggregateValue>,
) -> Result<()> {
    match value {
        Some(value) => {
            dst.push(1);
            encode_aggregate_value(dst, value)?;
        }
        None => dst.push(0),
    }
    Ok(())
}

pub(super) fn decode_optional_aggregate_value(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<Option<dbsp::AggregateValue>> {
    match read_u8(bytes, cursor)? {
        0 => Ok(None),
        1 => Ok(Some(decode_aggregate_value(bytes, cursor)?)),
        other => bail!("invalid optional aggregate value tag {other}"),
    }
}

pub(super) fn encode_aggregate_value(dst: &mut Vec<u8>, value: dbsp::AggregateValue) -> Result<()> {
    match value {
        dbsp::AggregateValue::Null(dbsp::AggregateValueType::Int64) => {
            dst.push(AGGREGATE_VALUE_NULL_INT64_TAG);
        }
        dbsp::AggregateValue::Null(dbsp::AggregateValueType::TimestampMillis) => {
            dst.push(AGGREGATE_VALUE_NULL_TIMESTAMP_MILLIS_TAG);
        }
        dbsp::AggregateValue::Null(dbsp::AggregateValueType::Utf8) => {
            dst.push(AGGREGATE_VALUE_NULL_UTF8_TAG);
        }
        dbsp::AggregateValue::Null(dbsp::AggregateValueType::DateDays) => {
            dst.push(AGGREGATE_VALUE_NULL_DATE_DAYS_TAG);
        }
        dbsp::AggregateValue::Null(dbsp::AggregateValueType::Decimal128 { precision, scale }) => {
            dst.push(AGGREGATE_VALUE_NULL_DECIMAL128_TAG);
            dst.push(precision);
            dst.push(scale as u8);
        }
        dbsp::AggregateValue::Int64(value) => {
            dst.push(AGGREGATE_VALUE_INT64_TAG);
            dst.extend_from_slice(&value.to_le_bytes());
        }
        dbsp::AggregateValue::TimestampMillis(value) => {
            dst.push(AGGREGATE_VALUE_TIMESTAMP_MILLIS_TAG);
            dst.extend_from_slice(&value.to_le_bytes());
        }
        dbsp::AggregateValue::Utf8(value) => {
            dst.push(AGGREGATE_VALUE_UTF8_TAG);
            write_len_prefixed_bytes(dst, value.as_bytes())?;
        }
        dbsp::AggregateValue::DateDays(value) => {
            dst.push(AGGREGATE_VALUE_DATE_DAYS_TAG);
            dst.extend_from_slice(&value.to_le_bytes());
        }
        dbsp::AggregateValue::Decimal128(value) => {
            dst.push(AGGREGATE_VALUE_DECIMAL128_TAG);
            dst.extend_from_slice(&value.to_le_bytes());
        }
    }
    Ok(())
}

pub(super) fn decode_aggregate_value(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<dbsp::AggregateValue> {
    match read_u8(bytes, cursor)? {
        AGGREGATE_VALUE_NULL_INT64_TAG => {
            Ok(dbsp::AggregateValue::Null(dbsp::AggregateValueType::Int64))
        }
        AGGREGATE_VALUE_NULL_TIMESTAMP_MILLIS_TAG => Ok(dbsp::AggregateValue::Null(
            dbsp::AggregateValueType::TimestampMillis,
        )),
        AGGREGATE_VALUE_NULL_UTF8_TAG => {
            Ok(dbsp::AggregateValue::Null(dbsp::AggregateValueType::Utf8))
        }
        AGGREGATE_VALUE_NULL_DATE_DAYS_TAG => Ok(dbsp::AggregateValue::Null(
            dbsp::AggregateValueType::DateDays,
        )),
        AGGREGATE_VALUE_NULL_DECIMAL128_TAG => Ok(dbsp::AggregateValue::Null(
            dbsp::AggregateValueType::Decimal128 {
                precision: read_u8(bytes, cursor)?,
                scale: read_u8(bytes, cursor)? as i8,
            },
        )),
        AGGREGATE_VALUE_INT64_TAG => Ok(dbsp::AggregateValue::Int64(read_i64_le(bytes, cursor)?)),
        AGGREGATE_VALUE_TIMESTAMP_MILLIS_TAG => Ok(dbsp::AggregateValue::TimestampMillis(
            read_i64_le(bytes, cursor)?,
        )),
        AGGREGATE_VALUE_UTF8_TAG => {
            let value = read_len_prefixed_bytes(bytes, cursor)?;
            Ok(dbsp::AggregateValue::Utf8(
                String::from_utf8(value).context("decode aggregate UTF-8 value")?,
            ))
        }
        AGGREGATE_VALUE_DATE_DAYS_TAG => {
            let end = cursor
                .checked_add(4)
                .ok_or_else(|| anyhow!("date-days cursor overflow"))?;
            if end > bytes.len() {
                bail!("truncated aggregate date-days value");
            }
            let value = i32::from_le_bytes(bytes[*cursor..end].try_into().unwrap());
            *cursor = end;
            Ok(dbsp::AggregateValue::DateDays(value))
        }
        AGGREGATE_VALUE_DECIMAL128_TAG => {
            let end = cursor
                .checked_add(16)
                .ok_or_else(|| anyhow!("decimal cursor overflow"))?;
            if end > bytes.len() {
                bail!("truncated aggregate decimal value");
            }
            let value = i128::from_le_bytes(bytes[*cursor..end].try_into().unwrap());
            *cursor = end;
            Ok(dbsp::AggregateValue::Decimal128(value))
        }
        other => bail!("unknown aggregate value tag {other}"),
    }
}

pub(super) fn write_len_prefixed_bytes(dst: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| anyhow!("byte field too large"))?;
    dst.extend_from_slice(&len.to_le_bytes());
    dst.extend_from_slice(bytes);
    Ok(())
}

pub(super) fn read_len_prefixed_bytes(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let len = read_u32_le(bytes, cursor)? as usize;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| anyhow!("length-prefixed byte field overflow"))?;
    if end > bytes.len() {
        bail!("truncated length-prefixed byte field");
    }
    let value = bytes[*cursor..end].to_vec();
    *cursor = end;
    Ok(value)
}

pub(super) fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8> {
    let value = *bytes.get(*cursor).ok_or_else(|| anyhow!("truncated u8"))?;
    *cursor = cursor
        .checked_add(1)
        .ok_or_else(|| anyhow!("u8 cursor overflow"))?;
    Ok(value)
}

pub(super) fn read_u32_le(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| anyhow!("u32 cursor overflow"))?;
    let chunk = bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow!("truncated u32"))?;
    *cursor = end;
    Ok(u32::from_le_bytes(chunk.try_into().unwrap()))
}

pub(super) fn read_i64_le(bytes: &[u8], cursor: &mut usize) -> Result<i64> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| anyhow!("i64 cursor overflow"))?;
    let chunk = bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow!("truncated i64"))?;
    *cursor = end;
    Ok(i64::from_le_bytes(chunk.try_into().unwrap()))
}

pub(super) fn read_i128_le(bytes: &[u8], cursor: &mut usize) -> Result<i128> {
    let end = cursor
        .checked_add(16)
        .ok_or_else(|| anyhow!("i128 cursor overflow"))?;
    let chunk = bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow!("truncated i128"))?;
    *cursor = end;
    Ok(i128::from_le_bytes(chunk.try_into().unwrap()))
}

pub(super) fn encode_count_aggregate_output_deltas(
    deltas: Vec<((Vec<u8>, Vec<i64>), i64)>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    let mut encoded = Vec::with_capacity(deltas.len());
    for ((key, values), diff) in deltas {
        if diff == 0 {
            continue;
        }
        let encoded_aggregate_values = encode_i64_values(&values)?;
        let row = concat_encoded_rows(&key, &encoded_aggregate_values)?;
        encoded.push((row, diff));
    }
    Ok(encoded)
}

pub(super) fn encode_incremental_aggregate_output_deltas(
    deltas: Vec<((Vec<u8>, Vec<dbsp::AggregateValue>), i64)>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    let mut encoded = Vec::with_capacity(deltas.len());
    for ((key, values), diff) in deltas {
        if diff == 0 {
            continue;
        }
        let encoded_aggregate_values = encode_incremental_aggregate_values(&values)?;
        let row = concat_encoded_rows(&key, &encoded_aggregate_values)?;
        encoded.push((row, diff));
    }
    Ok(encoded)
}

pub(super) fn merge_incremental_aggregate_output_deltas(
    target: &mut Vec<((Vec<u8>, Vec<dbsp::AggregateValue>), i64)>,
    updates: Vec<((Vec<u8>, Vec<dbsp::AggregateValue>), i64)>,
) {
    if updates.is_empty() {
        return;
    }

    let mut merged = HashMap::<(Vec<u8>, Vec<dbsp::AggregateValue>), i64>::new();
    for (row, delta) in target.drain(..).chain(updates) {
        if delta == 0 {
            continue;
        }
        let entry = merged.entry(row.clone()).or_insert(0);
        *entry += delta;
        if *entry == 0 {
            merged.remove(&row);
        }
    }
    target.extend(merged);
}

pub(super) fn encode_i64_values(values: &[i64]) -> Result<Vec<u8>> {
    let count = u32::try_from(values.len()).map_err(|_| anyhow!("too many columns in MV key"))?;
    let mut encoded = Vec::with_capacity(4 + (values.len() * 9));
    encoded.extend_from_slice(&count.to_le_bytes());
    for value in values {
        encoded.push(0x01);
        encoded.extend_from_slice(&value.to_le_bytes());
    }
    Ok(encoded)
}

pub(super) fn encode_incremental_aggregate_values(
    values: &[dbsp::AggregateValue],
) -> Result<Vec<u8>> {
    let count = u32::try_from(values.len()).map_err(|_| anyhow!("too many columns in MV key"))?;
    let mut encoded = Vec::with_capacity(4 + (values.len() * 9));
    encoded.extend_from_slice(&count.to_le_bytes());
    for value in values {
        match value {
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::Int64) => encoded.push(0x05),
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::TimestampMillis) => {
                encoded.push(0x07);
            }
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::Utf8) => encoded.push(0x06),
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::DateDays) => encoded.push(0x0A),
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::Decimal128 { .. }) => {
                encoded.push(0x0C);
            }
            dbsp::AggregateValue::Int64(value) => {
                encoded.push(0x01);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            dbsp::AggregateValue::TimestampMillis(value) => {
                encoded.push(0x03);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            dbsp::AggregateValue::Utf8(value) => {
                encoded.push(0x02);
                let bytes = value.as_bytes();
                let len = u32::try_from(bytes.len())
                    .map_err(|_| anyhow!("utf8 value too large for MV key"))?;
                encoded.extend_from_slice(&len.to_le_bytes());
                encoded.extend_from_slice(bytes);
            }
            dbsp::AggregateValue::DateDays(value) => {
                encoded.push(0x09);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            dbsp::AggregateValue::Decimal128(value) => {
                encoded.push(0x0B);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
        }
    }
    Ok(encoded)
}
