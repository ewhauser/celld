use celld_logic::disk_removal::{Control, Phase};

#[test]
fn acceptance_is_not_completion_and_operations_are_generation_bound() {
    let mut state = Control::new("g1".into());
    assert!(state.request("op", "g0").is_err());
    assert!(state.request("op", "g1").unwrap());
    assert!(!state.request("op", "g1").unwrap());
    assert!(state.request("other", "g1").is_err());
    assert_eq!(state.operation.as_ref().unwrap().phase, Phase::Draining);
    state.finish(Err("deadline".into()));
    state.finish(Ok(()));
    assert_eq!(state.operation.as_ref().unwrap().phase, Phase::Failed);
    assert!(!state.request("op", "g1").unwrap());
}

#[test]
fn every_obligation_and_join_is_required() {
    use celld_logic::disk_removal::may_complete;
    assert!(may_complete(true, true, [true, true]));
    assert!(!may_complete(false, true, [true]));
    assert!(!may_complete(true, false, [true]));
    assert!(!may_complete(true, true, [true, false]));
}

#[test]
fn coverage_is_epoch_specific_and_never_inferred_from_absence() {
    use celld_logic::disk_removal::{follower_covered, Coverage};
    assert!(!follower_covered(4, Coverage::Missing));
    assert!(!follower_covered(
        4,
        Coverage::Current {
            epoch: 3,
            sealed: true,
            bucket_complete: true
        }
    ));
    assert!(!follower_covered(
        4,
        Coverage::Current {
            epoch: 4,
            sealed: false,
            bucket_complete: false
        }
    ));
    assert!(follower_covered(
        4,
        Coverage::Current {
            epoch: 4,
            sealed: false,
            bucket_complete: true
        }
    ));
    assert!(follower_covered(
        4,
        Coverage::Current {
            epoch: 5,
            sealed: false,
            bucket_complete: false
        }
    ));
}

#[test]
fn bucket_proof_survives_recovery_but_new_epochs_require_a_new_barrier() {
    use celld_logic::log_tier::*;
    let mut log = create_record(["follower".to_string()].into_iter().collect(), 0).unwrap();
    log.bucket_complete = true;
    let recovering = start_recovery(&log, "survivor", 1).unwrap();
    assert!(recovering.bucket_complete);
    assert_eq!(takeover_gate(Some(&log)), TakeoverGate::RecoverFirst);
    assert!(finish_recovery(&recovering, 0).unwrap().bucket_complete);
    let replacement =
        plan_reconfigure(&log, 0, ["other".to_string()].into_iter().collect()).unwrap();
    assert!(!replacement.record.bucket_complete);
}
