// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Process-local strict shutdown state. The caller persists intent/results and
//! prevents restart before removing storage; an interrupted operation has no result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Phase {
    Draining,
    DataSafe,
    Failed,
}
#[derive(Clone, Debug)]
pub struct Operation {
    pub id: String,
    pub phase: Phase,
    pub blocker: Option<String>,
}
#[derive(Clone, Debug)]
pub struct Control {
    pub generation: String,
    pub operation: Option<Operation>,
    pub ordinary_shutdown: bool,
}
impl Control {
    pub fn new(generation: String) -> Self {
        Self {
            generation,
            operation: None,
            ordinary_shutdown: false,
        }
    }
    /// Returns true only for the first admission. Duplicate requests never
    /// restart work, clear failure, or alter a terminal result.
    pub fn request(&mut self, id: &str, generation: &str) -> Result<bool, &'static str> {
        if generation != self.generation {
            return Err("wrong runtime generation");
        }
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        {
            return Err("invalid operation id");
        }
        if self.ordinary_shutdown {
            return Err("ordinary shutdown already started");
        }
        if let Some(op) = &self.operation {
            return if op.id == id {
                Ok(false)
            } else {
                Err("conflicting shutdown operation")
            };
        }
        self.operation = Some(Operation {
            id: id.into(),
            phase: Phase::Draining,
            blocker: Some("draining application activity".into()),
        });
        Ok(true)
    }
    pub fn progress(&mut self, blocker: String) {
        if let Some(op) = &mut self.operation {
            if op.phase == Phase::Draining {
                op.blocker = Some(blocker);
            }
        }
    }
    pub fn finish(&mut self, result: Result<(), String>) {
        if let Some(op) = &mut self.operation {
            if op.phase != Phase::Draining {
                return;
            }
            match result {
                Ok(()) => {
                    op.phase = Phase::DataSafe;
                    op.blocker = None;
                }
                Err(error) => {
                    op.phase = Phase::Failed;
                    op.blocker = Some(error);
                }
            }
        }
    }
}
pub fn may_complete(
    joined: bool,
    own_covered: bool,
    followers: impl IntoIterator<Item = bool>,
) -> bool {
    joined && own_covered && followers.into_iter().all(|covered| covered)
}

/// Facts from a successfully decoded existing node lease, never a missing GET.
#[derive(Clone, Copy, Debug)]
pub enum Coverage {
    RecoveredSuccessor,
    Current {
        epoch: u64,
        sealed: bool,
        bucket_complete: bool,
    },
    Missing,
}
pub fn follower_covered(expected_epoch: u64, coverage: Coverage) -> bool {
    match coverage {
        Coverage::RecoveredSuccessor => true,
        Coverage::Current {
            epoch,
            sealed,
            bucket_complete,
        } => epoch > expected_epoch || (epoch == expected_epoch && (sealed || bucket_complete)),
        Coverage::Missing => false,
    }
}
