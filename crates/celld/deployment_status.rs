// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Read-only application deployment observations, never lifecycle authority.
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Deployment {
    pub version: String,
    pub prefix: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Target {
    #[serde(flatten)]
    pub deployment: Deployment,
    pub observed_at_ms: u64,
}

/// Updated only by the serialized pointer watcher. Failed reads retain the last
/// target for diagnostics but invalidate its authority to establish convergence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observation {
    pub revision: u64,
    pub target: Option<Target>,
    pub pointer_status: &'static str,
    pub adoption_status: &'static str,
}
impl Default for Observation {
    fn default() -> Self {
        Self {
            revision: 0,
            target: None,
            pointer_status: "unknown",
            adoption_status: "unknown",
        }
    }
}
impl Observation {
    pub fn observed(&mut self, version: String, prefix: String, now_ms: u64) {
        self.revision += 1;
        self.target = Some(Target {
            deployment: Deployment { version, prefix },
            observed_at_ms: now_ms,
        });
        self.pointer_status = "observed";
        self.adoption_status = "adopting";
    }
    pub fn unavailable(&mut self) {
        self.revision += 1;
        self.pointer_status = "unavailable";
        self.adoption_status = "unknown";
    }
    pub fn finished(&mut self, outcome: &'static str) {
        self.revision += 1;
        self.adoption_status = outcome;
    }
}

/// Counts from one decision-core turn. Generation is process-local and is used
/// only to ensure the census belongs to the loaded runtime generation.
#[derive(Default)]
pub struct Census {
    pub generation: u64,
    pub resident_cells: usize,
    pub pending_cells: usize,
    pub swapping_cells: usize,
}

#[derive(Serialize)]
pub struct Snapshot {
    pub schema_version: u32,
    pub runtime_generation: String,
    pub sampled_at_ms: u64,
    pub snapshot_valid: bool,
    pub loaded: Option<Deployment>,
    pub local_generation: u64,
    pub target: Option<Target>,
    pub pointer_status: &'static str,
    pub adoption_status: &'static str,
    pub resident_cells: usize,
    pub pending_cells: usize,
    pub swapping_cells: usize,
}
impl Snapshot {
    pub fn new(
        runtime_generation: String,
        sampled_at_ms: u64,
        loaded: Option<Deployment>,
        generations: (u64, u64),
        observations: (&Observation, &Observation),
        census: Option<Census>,
    ) -> Self {
        let (before, after) = observations;
        let valid = loaded.is_some()
            && generations.0 == generations.1
            && before == after
            && census
                .as_ref()
                .is_some_and(|c| c.generation == generations.0);
        let census = census.unwrap_or_default();
        Self {
            schema_version: 1,
            runtime_generation,
            sampled_at_ms,
            snapshot_valid: valid,
            loaded,
            local_generation: generations.0,
            target: after.target.clone(),
            pointer_status: after.pointer_status,
            adoption_status: after.adoption_status,
            resident_cells: census.resident_cells,
            pending_cells: census.pending_cells,
            swapping_cells: census.swapping_cells,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn observation() -> Observation {
        let mut o = Observation::default();
        o.observed("v2".into(), "deploy/v2/".into(), 100);
        o.finished("adopted");
        o
    }
    #[test]
    fn failed_read_preserves_target_but_invalidates_pointer() {
        let mut o = observation();
        o.unavailable();
        assert_eq!(o.target.as_ref().unwrap().deployment.version, "v2");
        assert_eq!(o.pointer_status, "unavailable");
        assert_eq!(o.adoption_status, "unknown");
    }
    #[test]
    fn build_failure_and_rollback_have_explicit_targets() {
        let mut o = observation();
        o.finished("failed");
        assert_eq!(o.adoption_status, "failed");
        o.observed("v1".into(), "deploy/v1/".into(), 200);
        assert_eq!(o.adoption_status, "adopting");
        o.finished("adopted");
        assert_eq!(o.target.unwrap().deployment.version, "v1");
    }
    #[test]
    fn snapshot_rejects_adoption_and_pointer_races() {
        let before = observation();
        let mut changed = before.clone();
        changed.observed("v3".into(), "deploy/v3/".into(), 200);
        for (after, generations, core, valid) in [
            (&before, (2, 2), 2, true),
            (&changed, (2, 2), 2, false),
            (&before, (2, 3), 2, false),
            (&before, (2, 2), 1, false),
        ] {
            let s = Snapshot::new(
                "process-a".into(),
                200,
                Some(before.target.as_ref().unwrap().deployment.clone()),
                generations,
                (&before, after),
                Some(Census {
                    generation: core,
                    resident_cells: 3,
                    pending_cells: 1,
                    swapping_cells: 1,
                }),
            );
            assert_eq!(s.snapshot_valid, valid);
            assert_eq!(s.pending_cells, 1); // Adoption does not erase lagging cells.
            assert_eq!(s.runtime_generation, "process-a");
        }
        let missing = Snapshot::new(
            "process-b".into(),
            200,
            None,
            (0, 0),
            (&before, &before),
            None,
        );
        assert!(!missing.snapshot_valid);
    }
}
