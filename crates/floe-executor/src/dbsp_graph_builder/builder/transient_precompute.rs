use super::*;

pub(super) fn build_transient_aggregate_precompute(
    aggregate: &DbspAggregateNode,
) -> Result<(
    Option<Arc<VectorizedFilterProjectEvaluator>>,
    Arc<RowSchema>,
    Arc<HashMap<String, usize>>,
)> {
    let input_schema = Arc::clone(aggregate.input_schema());
    let mut expressions = Vec::new();
    expressions.extend(
        aggregate
            .group_keys()
            .iter()
            .map(|group_key| group_key.expression().clone()),
    );
    for agg in aggregate.aggregates() {
        if let Some(filter) = agg.filter() {
            expressions.push(filter.clone());
        }
        if let Some(expr) = agg.expression() {
            expressions.push(expr.clone());
        }
    }

    let mut direct_input_columns = BTreeSet::new();
    let mut seen = HashSet::new();
    let mut non_direct_expressions = Vec::new();
    for expr in &expressions {
        if let Some(column_idx) =
            transient_aggregate_direct_column_index(expr, input_schema.as_ref())
        {
            direct_input_columns.insert(column_idx);
            continue;
        }
        let key = transient_aggregate_expression_lookup_key(expr.expr());
        if seen.insert(key.clone()) {
            non_direct_expressions.push((key, expr.expr().clone()));
        }
    }
    if non_direct_expressions.is_empty() {
        return Ok((None, input_schema, Arc::new(HashMap::new())));
    }

    let mut items = Vec::with_capacity(direct_input_columns.len() + non_direct_expressions.len());
    for column_idx in direct_input_columns {
        let field = input_schema
            .field(column_idx)
            .ok_or_else(|| anyhow!("transient aggregate input column {column_idx} missing"))?;
        items.push(dbsp::circuit::plan::ProjectItem {
            expr: Expr::Column(Column::new_unqualified(field.name.clone())),
            alias: Some(field.name.clone()),
        });
    }

    let mut expression_columns = HashMap::with_capacity(non_direct_expressions.len());
    let mut next_index = items.len();
    for (index, (key, expr)) in non_direct_expressions.into_iter().enumerate() {
        let alias = format!("__floe_transient_aggregate_expr_{index}");
        items.push(dbsp::circuit::plan::ProjectItem {
            expr,
            alias: Some(alias),
        });
        expression_columns.insert(key, next_index);
        next_index += 1;
    }

    let project_node = DbspProjectNode::try_new(Arc::clone(&input_schema), items)
        .context("build transient aggregate expression precompute projection")?;
    let evaluator = VectorizedFilterProjectEvaluator::for_map(
        project_node.expressions(),
        Arc::clone(&input_schema),
    )
    .context("initialize transient aggregate precompute evaluator")?;
    Ok((
        Some(Arc::new(evaluator)),
        Arc::clone(project_node.output_schema()),
        Arc::new(expression_columns),
    ))
}

pub(super) fn build_transient_window_count_star_precompute(
    window: &dbsp::DbspWindowAggregateNode,
) -> Result<(
    Option<Arc<VectorizedFilterProjectEvaluator>>,
    Arc<RowSchema>,
    Arc<HashMap<String, usize>>,
)> {
    let input_schema = Arc::clone(window.aggregate.input_schema());
    let mut expressions = Vec::new();
    expressions.extend(
        window
            .aggregate
            .group_keys()
            .iter()
            .map(|group_key| group_key.expression().clone()),
    );
    expressions.push(window.window.time_expression.clone());
    build_transient_expression_precompute(input_schema, expressions, "__floe_transient_window_expr")
}

pub(super) fn build_transient_window_aggregate_precompute(
    window: &dbsp::DbspWindowAggregateNode,
) -> Result<(
    Option<Arc<VectorizedFilterProjectEvaluator>>,
    Arc<RowSchema>,
    Arc<HashMap<String, usize>>,
)> {
    let input_schema = Arc::clone(window.aggregate.input_schema());
    let mut expressions = Vec::new();
    expressions.extend(
        window
            .aggregate
            .group_keys()
            .iter()
            .map(|group_key| group_key.expression().clone()),
    );
    expressions.push(window.window.time_expression.clone());
    for agg in window.aggregate.aggregates() {
        if let Some(filter) = agg.filter() {
            expressions.push(filter.clone());
        }
        if let Some(expr) = agg.expression() {
            expressions.push(expr.clone());
        }
    }
    build_transient_expression_precompute(
        input_schema,
        expressions,
        "__floe_transient_window_aggregate_expr",
    )
}

pub(super) fn build_transient_expression_precompute(
    input_schema: Arc<RowSchema>,
    expressions: Vec<DbspExpression>,
    alias_prefix: &str,
) -> Result<(
    Option<Arc<VectorizedFilterProjectEvaluator>>,
    Arc<RowSchema>,
    Arc<HashMap<String, usize>>,
)> {
    let mut direct_input_columns = BTreeSet::new();
    let mut seen = HashSet::new();
    let mut non_direct_expressions = Vec::new();
    for expr in &expressions {
        if let Some(column_idx) =
            transient_aggregate_direct_column_index(expr, input_schema.as_ref())
        {
            direct_input_columns.insert(column_idx);
            continue;
        }
        let key = transient_aggregate_expression_lookup_key(expr.expr());
        if seen.insert(key.clone()) {
            non_direct_expressions.push((key, expr.expr().clone()));
        }
    }
    if non_direct_expressions.is_empty() {
        return Ok((None, input_schema, Arc::new(HashMap::new())));
    }

    let mut items = Vec::with_capacity(direct_input_columns.len() + non_direct_expressions.len());
    for column_idx in direct_input_columns {
        let field = input_schema
            .field(column_idx)
            .ok_or_else(|| anyhow!("transient expression input column {column_idx} missing"))?;
        items.push(dbsp::circuit::plan::ProjectItem {
            expr: Expr::Column(Column::new_unqualified(field.name.clone())),
            alias: Some(field.name.clone()),
        });
    }

    let mut expression_columns = HashMap::with_capacity(non_direct_expressions.len());
    let mut next_index = items.len();
    for (index, (key, expr)) in non_direct_expressions.into_iter().enumerate() {
        let alias = format!("{alias_prefix}_{index}");
        items.push(dbsp::circuit::plan::ProjectItem {
            expr,
            alias: Some(alias),
        });
        expression_columns.insert(key, next_index);
        next_index += 1;
    }

    let project_node = DbspProjectNode::try_new(Arc::clone(&input_schema), items)
        .context("build transient expression precompute projection")?;
    let evaluator = VectorizedFilterProjectEvaluator::for_map(
        project_node.expressions(),
        Arc::clone(&input_schema),
    )
    .context("initialize transient expression precompute evaluator")?;
    Ok((
        Some(Arc::new(evaluator)),
        Arc::clone(project_node.output_schema()),
        Arc::new(expression_columns),
    ))
}

pub(super) fn transient_aggregate_direct_column_index(
    expression: &DbspExpression,
    schema: &RowSchema,
) -> Option<usize> {
    match expression.expr() {
        Expr::Alias(alias) => {
            transient_aggregate_direct_column_index_expression(alias.expr.as_ref(), schema)
        }
        other => transient_aggregate_direct_column_index_expression(other, schema),
    }
}

pub(super) fn transient_aggregate_direct_column_index_expression(
    expr: &Expr,
    schema: &RowSchema,
) -> Option<usize> {
    match expr {
        Expr::Column(column) => projection_resolve_direct_column(schema, column),
        Expr::Alias(alias) => {
            transient_aggregate_direct_column_index_expression(alias.expr.as_ref(), schema)
        }
        _ => None,
    }
}

pub(super) fn transient_aggregate_expression_lookup_key(expr: &Expr) -> String {
    match expr {
        Expr::Alias(alias) => transient_aggregate_expression_lookup_key(alias.expr.as_ref()),
        other => other.to_string(),
    }
}

pub(super) fn transient_window_direct_group_key_columns(
    group_keys: &[dbsp::circuit::plan::GroupKeyExpr],
    schema: &RowSchema,
    expression_columns: &HashMap<String, usize>,
) -> Option<Vec<usize>> {
    group_keys
        .iter()
        .map(|key_expr| {
            transient_window_resolved_expression_column_index(
                key_expr.expression(),
                schema,
                expression_columns,
            )
        })
        .collect()
}

pub(super) fn transient_window_resolved_expression_column_index(
    expression: &DbspExpression,
    schema: &RowSchema,
    expression_columns: &HashMap<String, usize>,
) -> Option<usize> {
    transient_aggregate_direct_column_index(expression, schema).or_else(|| {
        expression_columns
            .get(&transient_aggregate_expression_lookup_key(
                expression.expr(),
            ))
            .copied()
    })
}

pub(super) fn is_transient_window_count_star_root(window: &dbsp::DbspWindowAggregateNode) -> bool {
    if matches!(window.window.policy, dbsp::DbspWindowPolicy::Session { .. }) {
        return false;
    }
    let aggregates = window.aggregate.aggregates();
    aggregates.len() == 1
        && aggregates.iter().all(|agg| {
            agg.function() == &dbsp::DbspAggregateFunction::Count
                && !agg.distinct()
                && agg.filter().is_none()
                && agg.expression().is_none_or(|expr| match expr.expr() {
                    Expr::Literal(value, _) => !value.is_null(),
                    _ => false,
                })
        })
}

pub(super) fn is_transient_window_incremental_root(window: &dbsp::DbspWindowAggregateNode) -> bool {
    if matches!(window.window.policy, dbsp::DbspWindowPolicy::Session { .. }) {
        return false;
    }
    build_incremental_aggregate_slot_kinds(window.aggregate.aggregates()).is_some()
}

pub(super) fn transient_window_for_each_window<F>(
    ts: i64,
    window_size: i64,
    window_slide: i64,
    mut visit: F,
) where
    F: FnMut(i64, i64),
{
    if window_size == window_slide {
        let start = ts.div_euclid(window_slide) * window_slide;
        visit(start, start + window_size);
        return;
    }

    let latest_start = ts.div_euclid(window_slide) * window_slide;
    let count = (window_size / window_slide).max(1);
    let first_start = latest_start - (count - 1) * window_slide;
    for i in 0..count {
        let start = first_start + i * window_slide;
        visit(start, start + window_size);
    }
}

pub(super) fn transient_window_watermark_cutoff(
    watermark: &AtomicI64,
    allowed_lateness_ms: i64,
) -> Option<i64> {
    if allowed_lateness_ms == i64::MAX {
        return None;
    }
    let watermark = watermark.load(Ordering::Relaxed);
    if watermark < 0 {
        return None;
    }
    Some(watermark.saturating_sub(allowed_lateness_ms.max(0)))
}

pub(super) fn merge_i64_delta<K, S>(map: &mut HashMap<K, i64, S>, key: K, delta: i64)
where
    K: Eq + Hash,
    S: BuildHasher,
{
    if delta == 0 {
        return;
    }

    match map.entry(key) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            let merged = entry.get().saturating_add(delta);
            if merged == 0 {
                entry.remove();
            } else {
                *entry.get_mut() = merged;
            }
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(delta);
        }
    }
}

pub(super) fn apply_transient_window_count_delta(
    counts: &mut AHashMap<TransientWindowCountKey, i64>,
    eviction_schedule: &mut BTreeMap<i64, Vec<TransientWindowCountKey>>,
    updates: &mut TransientWindowCountUpdates,
    key: TransientWindowCountKey,
    delta: i64,
    track_evictions: bool,
) {
    if delta == 0 {
        return;
    }
    let old_count = counts.get(&key).copied().unwrap_or(0);
    let new_count = old_count.saturating_add(delta);
    if old_count == new_count {
        return;
    }
    if old_count != 0 {
        updates.merge(&key, old_count, -1);
    }
    if new_count != 0 {
        updates.merge(&key, new_count, 1);
        if track_evictions && old_count == 0 {
            eviction_schedule
                .entry(key.end)
                .or_default()
                .push(key.clone());
        }
        counts.insert(key, new_count);
    } else {
        counts.remove(&key);
    }
}

pub(super) fn transient_window_evict_expired_counts(
    cutoff: Option<i64>,
    counts: &mut AHashMap<TransientWindowCountKey, i64>,
    eviction_schedule: &mut BTreeMap<i64, Vec<TransientWindowCountKey>>,
    updates: &mut TransientWindowCountUpdates,
) {
    let Some(cutoff) = cutoff else {
        return;
    };
    let retained = eviction_schedule.split_off(&(cutoff + 1));
    let expired = std::mem::replace(eviction_schedule, retained);
    for (_, keys) in expired {
        for key in keys {
            let Some(old_count) = counts.remove(&key) else {
                continue;
            };
            updates.merge(&key, old_count, -1);
        }
    }
}

pub(super) fn apply_transient_window_count_star_deltas(
    input_deltas: Vec<(Vec<u8>, i64)>,
    key_extractor: &VectorizedEncodedKeyExtractor,
    time_column: usize,
    window_size: i64,
    window_slide: i64,
    cutoff: Option<i64>,
    output_projection: Option<TransientWindowCountOutputProjection>,
    counts: &mut AHashMap<TransientWindowCountKey, i64>,
    eviction_schedule: &mut BTreeMap<i64, Vec<TransientWindowCountKey>>,
    track_evictions: bool,
) -> Result<TransientWindowCountUpdates> {
    let mut grouped_deltas: AHashMap<TransientWindowCountKey, i64> = AHashMap::new();
    let mut batch_group_key_intern: AHashMap<Vec<u8>, Arc<[u8]>> = AHashMap::new();
    for (_row, weight, raw_key, event_ts) in
        key_extractor.extract_keyed_time_deltas(&input_deltas, time_column)?
    {
        if weight == 0 {
            continue;
        }
        if event_ts < 0 {
            continue;
        }
        if let Some(cutoff) = cutoff
            && event_ts < cutoff
        {
            continue;
        }
        let key = match batch_group_key_intern.get(raw_key.as_slice()) {
            Some(key) => Arc::clone(key),
            None => {
                let key = Arc::<[u8]>::from(raw_key.clone().into_boxed_slice());
                batch_group_key_intern.insert(raw_key, Arc::clone(&key));
                key
            }
        };
        transient_window_for_each_window(event_ts, window_size, window_slide, |start, end| {
            merge_i64_delta(
                &mut grouped_deltas,
                TransientWindowCountKey {
                    start,
                    end,
                    key: Arc::clone(&key),
                },
                weight,
            );
        });
    }

    let mut updates = TransientWindowCountUpdates::new(output_projection);
    for (key, delta) in grouped_deltas {
        apply_transient_window_count_delta(
            counts,
            eviction_schedule,
            &mut updates,
            key,
            delta,
            track_evictions,
        );
    }

    if track_evictions {
        transient_window_evict_expired_counts(cutoff, counts, eviction_schedule, &mut updates);
    }
    Ok(updates)
}

pub(super) fn encode_transient_window_count_state(
    counts: &AHashMap<TransientWindowCountKey, i64>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    counts
        .iter()
        .filter(|(_, count)| **count != 0)
        .map(|(key, count)| {
            let encoded_window = encode_transient_window_bounds(key.start, key.end)?;
            let row = concat_encoded_rows(&encoded_window, &key.key)?;
            Ok((row, *count))
        })
        .collect()
}

pub(super) fn restore_transient_window_count_state(
    rows: Vec<(Vec<u8>, i64)>,
    counts: &mut AHashMap<TransientWindowCountKey, i64>,
    eviction_schedule: &mut BTreeMap<i64, Vec<TransientWindowCountKey>>,
    track_evictions: bool,
) -> Result<()> {
    for (row, count) in rows {
        if count == 0 {
            continue;
        }
        let key = decode_transient_window_count_state_key(&row)?;
        counts.insert(key.clone(), count);
        if track_evictions {
            eviction_schedule.entry(key.end).or_default().push(key);
        }
    }
    Ok(())
}

pub(super) fn decode_transient_window_count_state_key(
    row: &[u8],
) -> Result<TransientWindowCountKey> {
    if row.len() < 4 {
        bail!("encoded window count state row too short");
    }
    let column_count = u32::from_le_bytes(row[0..4].try_into().unwrap()) as usize;
    if column_count < 2 {
        bail!("encoded window count state row has fewer than two window columns");
    }
    let start = extract_encoded_row_i64_like_column(row, 0)?
        .ok_or_else(|| anyhow!("encoded window count state start is null"))?;
    let end = extract_encoded_row_i64_like_column(row, 1)?
        .ok_or_else(|| anyhow!("encoded window count state end is null"))?;
    let key_columns = (2..column_count).collect::<Vec<_>>();
    let key = extract_encoded_row_columns(row, &key_columns, false)?
        .ok_or_else(|| anyhow!("encoded window count state key unexpectedly null"))?;
    Ok(TransientWindowCountKey {
        start,
        end,
        key: Arc::<[u8]>::from(key.into_boxed_slice()),
    })
}

pub(super) fn transient_window_encoded_key_end(row: &[u8]) -> Result<i64> {
    extract_encoded_row_i64_like_column(row, 1)?
        .ok_or_else(|| anyhow!("encoded window key end is null"))
}

pub(super) fn encode_transient_window_count_output_deltas(
    deltas: TransientWindowCountUpdates,
) -> Result<Vec<(Vec<u8>, i64)>> {
    let deltas = match deltas {
        TransientWindowCountUpdates::Full(deltas) => deltas,
        TransientWindowCountUpdates::GroupKeyAndCount(deltas) => {
            return encode_transient_window_count_group_key_count_output_deltas(deltas);
        }
    };
    let mut encoded = Vec::with_capacity(deltas.len());
    for ((key, count), diff) in deltas {
        if diff == 0 {
            continue;
        }
        let encoded_window = encode_transient_window_bounds(key.start, key.end)?;
        let with_key = concat_encoded_rows(&encoded_window, &key.key)?;
        let encoded_count = encode_i64_values(std::slice::from_ref(&count))?;
        let row = concat_encoded_rows(&with_key, &encoded_count)?;
        encoded.push((row, diff));
    }
    Ok(encoded)
}

pub(super) fn encode_transient_window_count_group_key_count_output_deltas(
    deltas: AHashMap<(Arc<[u8]>, i64), i64>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    let mut projected = Vec::with_capacity(deltas.len());
    for ((key, count), diff) in deltas {
        if diff == 0 {
            continue;
        }
        let row = encode_transient_window_group_key_count_output_row(&key, count)?;
        projected.push((row, diff));
    }
    Ok(projected)
}

pub(super) fn encode_transient_window_group_key_count_output_row(
    group_key: &[u8],
    count: i64,
) -> Result<Vec<u8>> {
    if group_key.len() < 4 {
        bail!("transient window count group key is too short");
    }
    let group_key_count = transient_encoded_row_declared_column_count(group_key)?;
    let output_count = group_key_count
        .checked_add(1)
        .ok_or_else(|| anyhow!("too many columns in MV key"))?;
    let output_count =
        u32::try_from(output_count).map_err(|_| anyhow!("too many columns in MV key"))?;
    let mut row = Vec::with_capacity(group_key.len() + 9);
    row.extend_from_slice(&output_count.to_le_bytes());
    row.extend_from_slice(&group_key[4..]);
    row.push(0x01);
    row.extend_from_slice(&count.to_le_bytes());
    Ok(row)
}

pub(super) fn transient_encoded_row_declared_column_count(row: &[u8]) -> Result<usize> {
    if row.len() < 4 {
        bail!("encoded key too short");
    }
    Ok(u32::from_le_bytes(row[0..4].try_into().unwrap()) as usize)
}

pub(super) fn encode_transient_window_bounds(start: i64, end: i64) -> Result<Vec<u8>> {
    let mut encoded = Vec::with_capacity(4 + 18);
    encoded.extend_from_slice(&2_u32.to_le_bytes());
    encoded.push(0x03);
    encoded.extend_from_slice(&start.to_le_bytes());
    encoded.push(0x03);
    encoded.extend_from_slice(&end.to_le_bytes());
    Ok(encoded)
}

pub(super) fn encode_transient_window_aggregate_input_pair(
    window_key: &[u8],
    row: &[u8],
) -> Result<Vec<u8>> {
    let key_len =
        u32::try_from(window_key.len()).context("transient window aggregate key too large")?;
    let mut encoded = Vec::with_capacity(4 + window_key.len() + row.len());
    encoded.extend_from_slice(&key_len.to_le_bytes());
    encoded.extend_from_slice(window_key);
    encoded.extend_from_slice(row);
    Ok(encoded)
}

pub(super) fn decode_transient_window_aggregate_input_pair(
    encoded: &[u8],
) -> Result<(Vec<u8>, Vec<u8>)> {
    if encoded.len() < 4 {
        bail!("transient window aggregate input pair missing key length");
    }
    let mut key_len = [0_u8; 4];
    key_len.copy_from_slice(&encoded[..4]);
    let key_len = u32::from_le_bytes(key_len) as usize;
    if encoded.len() < 4 + key_len {
        bail!("transient window aggregate input pair truncated");
    }
    Ok((
        encoded[4..4 + key_len].to_vec(),
        encoded[4 + key_len..].to_vec(),
    ))
}
