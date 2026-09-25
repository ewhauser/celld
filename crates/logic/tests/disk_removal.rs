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

#[test]
fn a_current_member_is_obligated_until_its_epoch_is_sealed_or_bucket_complete() {
    use celld_logic::disk_removal::current_member_obligated;
    assert!(current_member_obligated(4, false, false));
    assert!(!current_member_obligated(4, true, false));
    assert!(!current_member_obligated(4, false, true));
}

#[test]
fn fleet_view_reports_dead_unsealed_logs_and_every_pending_member() {
    use celld_logic::log_tier::*;
    fn observed(
        session: &str,
        expires_ms: u64,
        state: LogState,
        members: &[&str],
        bucket_complete: bool,
    ) -> ObservedLog {
        ObservedLog {
            session: session.into(),
            lease_expires_ms: expires_ms,
            record: LogRecord {
                epoch: 3,
                ensemble: members.iter().map(|member| member.to_string()).collect(),
                tiered: 0,
                bucket_complete,
                state,
                claimant: (state == LogState::Recovering).then(|| "rescuer".to_string()),
                claimed_ms: None,
            },
        }
    }
    let now = 1_000;
    let logs = [
        // A live leader still needs both of its followers.
        observed("live/g", now + 1, LogState::Open, &["a", "b"], false),
        // A live leader proven bucket-complete needs neither.
        observed("proven/g", now + 1, LogState::Open, &["a"], true),
        // A dead open log is unrecovered, and its members still hold it.
        observed("dead/g", now, LogState::Open, &["b"], false),
        // Another node is recovering this one; it is still unrecovered.
        observed("claimed/g", 1, LogState::Recovering, &["c"], false),
        // A dead bucket-complete log still needs its seal, not a member.
        observed("complete/g", 1, LogState::Open, &["d"], true),
        // A sealed log owes nothing.
        observed("sealed/g", 1, LogState::Sealed, &["a"], false),
    ];
    let view = fleet_log_view(&logs, now);
    assert_eq!(
        view.unrecovered
            .iter()
            .map(|log| (log.session.as_str(), log.state, log.claimant.as_deref()))
            .collect::<Vec<_>>(),
        [
            ("claimed/g", LogState::Recovering, Some("rescuer")),
            ("complete/g", LogState::Open, None),
            ("dead/g", LogState::Open, None),
        ]
    );
    let obligations = view
        .obligations
        .iter()
        .map(|(member, sessions)| {
            (
                member.as_str(),
                sessions.iter().map(String::as_str).collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        obligations,
        [
            ("a", vec!["live/g"]),
            ("b", vec!["dead/g", "live/g"]),
            ("c", vec!["claimed/g"]),
        ]
    );
}
