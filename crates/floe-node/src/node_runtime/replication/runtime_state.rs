use super::ReplicationPipelineRuntime;
use super::target_state::truncate_target_error;

pub(super) struct ReplicationReplayStateGuard<'a> {
    runtime: &'a ReplicationPipelineRuntime,
    pipeline_name: String,
}

impl<'a> ReplicationReplayStateGuard<'a> {
    pub(super) fn new(runtime: &'a ReplicationPipelineRuntime, pipeline_name: &str) -> Self {
        runtime.set_replay_state(pipeline_name, true);
        Self {
            runtime,
            pipeline_name: pipeline_name.to_string(),
        }
    }
}

impl Drop for ReplicationReplayStateGuard<'_> {
    fn drop(&mut self) {
        self.runtime.set_replay_state(&self.pipeline_name, false);
    }
}

impl ReplicationPipelineRuntime {
    pub(super) fn set_replay_state(&self, pipeline_name: &str, replaying: bool) {
        crate::metrics::record_cdc_replication_replaying(pipeline_name, replaying);
        match self.replay_state_by_pipeline.lock() {
            Ok(mut state) => {
                state.insert(pipeline_name.to_string(), replaying);
            }
            Err(_) => {
                tracing::warn!(
                    pipeline = %pipeline_name,
                    replaying,
                    "replication pipeline replay state lock poisoned"
                );
            }
        }
    }

    pub(super) fn set_source_backpressure_state(&self, pipeline_name: &str, active: bool) {
        crate::metrics::record_cdc_buffer_source_backpressure_active(pipeline_name, active);
        match self.backpressure_state_by_pipeline.lock() {
            Ok(mut state) => {
                state.insert(pipeline_name.to_string(), active);
            }
            Err(_) => {
                tracing::warn!(
                    pipeline = %pipeline_name,
                    active,
                    "replication pipeline backpressure state lock poisoned"
                );
            }
        }
    }

    pub(super) fn set_last_target_error(&self, pipeline_name: &str, error: String) {
        crate::metrics::record_cdc_replication_target_error(pipeline_name, true);
        match self.last_target_error_by_pipeline.lock() {
            Ok(mut errors) => {
                errors.insert(pipeline_name.to_string(), truncate_target_error(&error));
            }
            Err(_) => {
                tracing::warn!(
                    pipeline = %pipeline_name,
                    "replication pipeline target error state lock poisoned"
                );
            }
        }
    }

    pub(super) fn clear_last_target_error(&self, pipeline_name: &str) {
        crate::metrics::record_cdc_replication_target_error(pipeline_name, false);
        if let Ok(mut errors) = self.last_target_error_by_pipeline.lock() {
            errors.remove(pipeline_name);
        }
    }
}
