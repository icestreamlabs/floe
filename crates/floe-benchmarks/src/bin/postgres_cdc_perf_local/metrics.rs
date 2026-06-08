use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Default)]
pub(super) struct CdcMetrics {
    values: BTreeMap<String, String>,
}

impl CdcMetrics {
    pub(super) fn from_file(path: &Path) -> Self {
        let mut values = BTreeMap::new();
        let has_metrics = path.metadata().is_ok_and(|metadata| metadata.len() > 0);
        for (key, metric, label_filter) in CDC_PROM_METRICS {
            if let Some(value) = prom_metric_sum(path, metric, *label_filter) {
                values.insert((*key).to_string(), value);
            } else if has_metrics {
                values.insert((*key).to_string(), "0".to_string());
            }
        }
        let latency_sum = values
            .get("cdc_target_write_latency_sum_ms")
            .and_then(|value| value.parse::<f64>().ok());
        let latency_count = values
            .get("cdc_target_write_latency_count")
            .and_then(|value| value.parse::<f64>().ok());
        if let (Some(sum), Some(count)) = (latency_sum, latency_count)
            && count > 0.0
        {
            values.insert(
                "cdc_target_write_latency_avg_ms".to_string(),
                format!("{:.3}", sum / count),
            );
        }
        Self { values }
    }

    pub(super) fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }
}

pub(super) fn cdc_summary_keys() -> impl Iterator<Item = &'static str> {
    CDC_PROM_METRICS.iter().map(|(key, _, _)| *key)
}

const CDC_PROM_METRICS: &[(&str, &str, Option<&str>)] = &[
    (
        "cdc_buffer_pending_records",
        "floe_cdc_buffer_pending_records",
        None,
    ),
    (
        "cdc_buffer_pending_bytes",
        "floe_cdc_buffer_pending_bytes",
        None,
    ),
    (
        "cdc_buffer_appended_records",
        "floe_cdc_buffer_appended_records_total",
        None,
    ),
    (
        "cdc_buffer_appended_bytes",
        "floe_cdc_buffer_appended_bytes_total",
        None,
    ),
    (
        "cdc_buffer_append_latency_count",
        "floe_cdc_buffer_append_latency_ms_count",
        None,
    ),
    (
        "cdc_buffer_append_latency_sum_ms",
        "floe_cdc_buffer_append_latency_ms_sum",
        None,
    ),
    (
        "cdc_buffer_forced_flushes",
        "floe_cdc_buffer_forced_flushes_total",
        None,
    ),
    (
        "cdc_buffer_flush_latency_count",
        "floe_cdc_buffer_flush_latency_ms_count",
        None,
    ),
    (
        "cdc_buffer_flush_latency_sum_ms",
        "floe_cdc_buffer_flush_latency_ms_sum",
        None,
    ),
    (
        "cdc_buffer_replayed_records",
        "floe_cdc_buffer_replayed_records_total",
        None,
    ),
    (
        "cdc_buffer_replay_latency_count",
        "floe_cdc_buffer_replay_latency_ms_count",
        Some("phase=\"total\""),
    ),
    (
        "cdc_buffer_replay_latency_sum_ms",
        "floe_cdc_buffer_replay_latency_ms_sum",
        Some("phase=\"total\""),
    ),
    (
        "cdc_buffer_replay_delivery_latency_count",
        "floe_cdc_buffer_replay_latency_ms_count",
        Some("phase=\"target_delivery\""),
    ),
    (
        "cdc_buffer_replay_delivery_latency_sum_ms",
        "floe_cdc_buffer_replay_latency_ms_sum",
        Some("phase=\"target_delivery\""),
    ),
    (
        "cdc_buffer_replay_payload_load_latency_count",
        "floe_cdc_buffer_replay_latency_ms_count",
        Some("phase=\"payload_load\""),
    ),
    (
        "cdc_buffer_replay_payload_load_latency_sum_ms",
        "floe_cdc_buffer_replay_latency_ms_sum",
        Some("phase=\"payload_load\""),
    ),
    (
        "cdc_buffer_replay_encode_latency_count",
        "floe_cdc_buffer_replay_latency_ms_count",
        Some("phase=\"encode\""),
    ),
    (
        "cdc_buffer_replay_encode_latency_sum_ms",
        "floe_cdc_buffer_replay_latency_ms_sum",
        Some("phase=\"encode\""),
    ),
    (
        "cdc_buffer_object_create_count",
        "floe_cdc_buffer_object_ops_total",
        Some("operation=\"create\""),
    ),
    (
        "cdc_buffer_object_get_count",
        "floe_cdc_buffer_object_ops_total",
        Some("operation=\"get\""),
    ),
    (
        "cdc_buffer_object_delete_count",
        "floe_cdc_buffer_object_ops_total",
        Some("operation=\"delete\""),
    ),
    (
        "cdc_buffer_drain_attempts",
        "floe_cdc_buffer_drain_attempts_total",
        None,
    ),
    (
        "cdc_target_write_success_records",
        "floe_cdc_replication_target_write_records_total",
        Some("result=\"success\""),
    ),
    (
        "cdc_target_write_failure_records",
        "floe_cdc_replication_target_write_records_total",
        Some("result=\"failure\""),
    ),
    (
        "cdc_target_write_latency_count",
        "floe_cdc_replication_target_write_latency_ms_count",
        Some("result=\"success\""),
    ),
    (
        "cdc_target_write_latency_sum_ms",
        "floe_cdc_replication_target_write_latency_ms_sum",
        Some("result=\"success\""),
    ),
    (
        "cdc_target_write_batch_records_sum",
        "floe_cdc_replication_target_write_batch_records_sum",
        Some("result=\"success\""),
    ),
];

fn prom_metric_sum(path: &Path, metric: &str, label_filter: Option<&str>) -> Option<String> {
    let content = fs::read_to_string(path).ok()?;
    let mut sum = 0.0;
    let mut seen = false;
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let matches_metric = line.strip_prefix(metric).is_some_and(|rest| {
            rest.starts_with('{') || rest.chars().next().is_some_and(char::is_whitespace)
        });
        if !matches_metric {
            continue;
        }
        if let Some(label_filter) = label_filter
            && !line.contains(label_filter)
        {
            continue;
        }
        let Some(raw_value) = line.split_whitespace().last() else {
            continue;
        };
        if let Ok(value) = raw_value.parse::<f64>() {
            sum += value;
            seen = true;
        }
    }
    seen.then(|| {
        if (sum - sum.round()).abs() < f64::EPSILON {
            format!("{sum:.0}")
        } else {
            format!("{sum:.6}")
        }
    })
}
