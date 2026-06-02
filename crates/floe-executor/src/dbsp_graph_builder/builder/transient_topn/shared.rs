use super::*;

pub(super) fn accumulate_weight_deltas(
    output_deltas: &mut HashMap<Vec<u8>, i64>,
    previous_output: &HashMap<Vec<u8>, i64>,
    next_output: &HashMap<Vec<u8>, i64>,
) {
    for (row_key, previous_weight) in previous_output {
        let next_weight = next_output.get(row_key).copied().unwrap_or(0);
        let delta = next_weight.saturating_sub(*previous_weight);
        if delta != 0 {
            let entry = output_deltas.entry(row_key.clone()).or_insert(0);
            *entry = entry.saturating_add(delta);
            if *entry == 0 {
                output_deltas.remove(row_key);
            }
        }
    }
    for (row_key, next_weight) in next_output {
        if previous_output.contains_key(row_key) {
            continue;
        }
        if *next_weight != 0 {
            let entry = output_deltas.entry(row_key.clone()).or_insert(0);
            *entry = entry.saturating_add(*next_weight);
            if *entry == 0 {
                output_deltas.remove(row_key);
            }
        }
    }
}

pub(super) fn accumulate_single_weight_delta(
    output_deltas: &mut HashMap<Vec<u8>, i64>,
    row_key: Vec<u8>,
    diff: i64,
) {
    if diff == 0 {
        return;
    }
    let entry = output_deltas.entry(row_key.clone()).or_insert(0);
    *entry = entry.saturating_add(diff);
    if *entry == 0 {
        output_deltas.remove(&row_key);
    }
}
