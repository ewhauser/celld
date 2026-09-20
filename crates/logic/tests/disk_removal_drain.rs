//! Exercise the real core release pipeline without any successor node.
use celld_logic::{
    on_event, pressure::PressureConfig, CasOutcome, Config, Effect, Event, Failure,
    OwnershipOnEvict, Phase, RestoreOutcome, Route, State,
};
use std::collections::{BTreeSet, VecDeque};

fn state() -> State {
    State::new(
        "last-node",
        Config {
            max_resident: 16,
            max_activations: 16,
            max_evictions: 2,
            max_releases: 2,
            max_outbound_websockets: 16,
            ownership_on_evict: OwnershipOnEvict::Sticky,
            require_node_lease: false,
            peer_protocol: 1,
            operation_deadline_ms: None,
            owner_log_recovery_backoff_ms: 100,
            owner_log_recovery_attempts: 3,
            alarm_resident_ms: 0,
            idle_evict_ms: None,
            pressure: PressureConfig::default(),
        },
    )
}

fn populate(state: &mut State, count: u64) {
    for request in 1..=count {
        let mut effects: VecDeque<_> = on_event(
            state,
            Event::Request {
                request,
                cell: format!("cell-{request}"),
            },
        )
        .into();
        let mut routed = false;
        while let Some(effect) = effects.pop_front() {
            let event = match effect {
                Effect::ReadOwner { op, .. } => Event::OwnerRead {
                    op,
                    now_ms: 0,
                    result: Ok(None),
                },
                Effect::ReadCapacityPeers { op, .. } => Event::CapacityPeersRead {
                    op,
                    now_ms: 0,
                    result: Ok(vec![]),
                },
                Effect::CasOwner { op, .. } => Event::OwnerCasCompleted {
                    op,
                    result: Ok(CasOutcome::Applied),
                },
                Effect::Restore { op, .. } => Event::RestoreCompleted {
                    op,
                    result: Ok(RestoreOutcome {
                        restored: false,
                        alarm: None,
                    }),
                },
                Effect::StartRuntime { op, .. } => Event::RuntimeStarted {
                    op,
                    isolate: None,
                    generation: 1,
                    result: Ok(()),
                },
                Effect::Publish { op, .. } => Event::Published { op, result: Ok(()) },
                Effect::Complete {
                    result: Ok(Route::Local),
                    ..
                } => {
                    routed = true;
                    continue;
                }
                Effect::ScheduleTimer { .. } | Effect::ReconcileWakeEntry { .. } => continue,
                effect => panic!("unexpected activation effect: {effect:?}"),
            };
            effects.extend(on_event(state, event));
            state.validate().unwrap();
        }
        assert!(routed);
        on_event(state, Event::ActivityFinished { request });
    }
    assert_eq!(state.occupied(), count as usize);
}

#[test]
fn populated_last_node_releases_multiple_cohorts_without_a_successor() {
    let mut state = state();
    populate(&mut state, 7); // More than three max_releases cohorts.
    let mut effects: VecDeque<_> = on_event(&mut state, Event::ReleaseAllForDiskRemoval).into();
    let (mut durable, mut stopped, mut released) =
        (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
    while let Some(effect) = effects.pop_front() {
        assert!(state.handoffs_in_flight() <= 2);
        let event = match effect {
            Effect::CancelCellActivity { .. }
            | Effect::ScheduleTimer { .. }
            | Effect::ReconcileWakeEntry { .. } => continue,
            Effect::EnsureDurable {
                op,
                cell,
                revocable,
                ..
            } => {
                assert!(!revocable);
                assert!(durable.insert(cell));
                Event::DurabilityChecked { op, result: Ok(()) }
            }
            Effect::StopRuntime { op, cell, .. } => {
                assert!(durable.contains(&cell));
                assert!(stopped.insert(cell));
                Event::RuntimeStopped { op }
            }
            Effect::ReleaseOwner { op, cell, .. } => {
                assert!(stopped.contains(&cell));
                assert!(released.insert(cell));
                Event::OwnerReleased {
                    op,
                    result: Ok(CasOutcome::Applied),
                }
            }
            effect => panic!("disk removal must not need a successor: {effect:?}"),
        };
        effects.extend(on_event(&mut state, event));
        state.validate().unwrap();
    }
    assert_eq!(released.len(), 7);
    assert_eq!(state.handoffs_in_flight(), 0);
    assert_eq!(state.occupied(), 0);
    assert_eq!(state.drain_progress(), 7);
    assert_eq!(state.handed_off(), 0); // Do not report nonexistent peer adoption.
    for i in 1..=7 {
        assert_eq!(state.phase(&format!("cell-{i}")), Some(&Phase::Inactive));
    }
}

#[test]
fn ordinary_handoff_still_requires_successor_adoption() {
    let mut state = state();
    populate(&mut state, 1);
    let mut effects: VecDeque<_> = on_event(&mut state, Event::ReleaseAll).into();
    let mut adoption = false;
    while let Some(effect) = effects.pop_front() {
        let event = match effect {
            Effect::EnsureDurable { op, .. } => Event::DurabilityChecked { op, result: Ok(()) },
            Effect::StopRuntime { op, .. } => Event::RuntimeStopped { op },
            Effect::ReleaseOwner { op, .. } => Event::OwnerReleased {
                op,
                result: Ok(CasOutcome::Applied),
            },
            Effect::AdoptReleased { .. } => {
                adoption = true;
                continue;
            }
            Effect::CancelCellActivity { .. }
            | Effect::ScheduleTimer { .. }
            | Effect::ReconcileWakeEntry { .. } => continue,
            effect => panic!("unexpected effect: {effect:?}"),
        };
        effects.extend(on_event(&mut state, event));
    }
    assert!(adoption);
    assert_eq!(state.adopting(), 1);
    assert_eq!(state.drain_progress(), 0);
}

#[test]
fn disk_removal_does_not_skip_failed_durability() {
    let mut state = state();
    populate(&mut state, 1);
    let effects = on_event(&mut state, Event::ReleaseAllForDiskRemoval);
    let op = effects
        .iter()
        .find_map(|e| match e {
            Effect::EnsureDurable { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let effects = on_event(
        &mut state,
        Event::DurabilityChecked {
            op,
            result: Err(Failure::Definite),
        },
    );
    assert!(!effects.iter().any(|e| matches!(
        e,
        Effect::StopRuntime { .. } | Effect::ReleaseOwner { .. } | Effect::AdoptReleased { .. }
    )));
    assert_eq!(state.occupied(), 1);
    assert_eq!(state.drain_progress(), 0);
}

#[test]
fn strict_drain_settles_preexisting_rebalance_adoption_and_ignores_late_reply() {
    let mut state = state();
    populate(&mut state, 1);
    let effects = on_event(
        &mut state,
        Event::Evict {
            cell: "cell-1".into(),
        },
    );
    let op = effects
        .iter()
        .find_map(|e| match e {
            Effect::EnsureDurable { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let effects = on_event(&mut state, Event::DurabilityChecked { op, result: Ok(()) });
    let op = effects
        .iter()
        .find_map(|e| match e {
            Effect::StopRuntime { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    on_event(&mut state, Event::RuntimeStopped { op });
    assert!(matches!(state.phase("cell-1"), Some(Phase::Dormant { .. })));
    let mut effects: VecDeque<_> = on_event(&mut state, Event::Rebalance { cells: 1 }).into();
    let mut adoption_op = None;
    while let Some(effect) = effects.pop_front() {
        let event = match effect {
            Effect::EnsureDurable { op, .. } => Event::DurabilityChecked { op, result: Ok(()) },
            Effect::StopRuntime { op, .. } => Event::RuntimeStopped { op },
            Effect::ReleaseOwner { op, .. } => Event::OwnerReleased {
                op,
                result: Ok(CasOutcome::Applied),
            },
            Effect::AdoptReleased { op, rebalance, .. } => {
                assert!(rebalance);
                adoption_op = Some(op);
                continue;
            }
            Effect::CancelCellActivity { .. }
            | Effect::ScheduleTimer { .. }
            | Effect::ReconcileWakeEntry { .. } => continue,
            effect => panic!("unexpected rebalance effect: {effect:?}"),
        };
        effects.extend(on_event(&mut state, event));
    }
    let op = adoption_op.expect("rebalance waiting for a successor");
    assert_eq!(state.adopting(), 1);
    let effects = on_event(&mut state, Event::ReleaseAllForDiskRemoval);
    assert!(!effects.iter().any(|e| matches!(
        e,
        Effect::AdoptReleased { .. } | Effect::StartRuntime { .. }
    )));
    state.validate().unwrap();
    assert_eq!(state.adopting(), 0);
    assert_eq!(state.drain_progress(), 1);
    assert_eq!(state.phase("cell-1"), Some(&Phase::Inactive));
    on_event(
        &mut state,
        Event::SuccessorAdopted {
            op,
            result: Ok(celld_logic::AdoptedCell {
                node: "peer".into(),
                addr: "127.0.0.1:9000".into(),
                epoch: 2,
                peer_protocol: 1,
            }),
        },
    );
    state.validate().unwrap();
    assert_eq!(state.phase("cell-1"), Some(&Phase::Inactive));
    assert_eq!(state.drain_progress(), 1);
    assert_eq!(state.handed_off(), 0);
}

#[test]
fn disk_removal_waits_for_runtime_stop_and_does_not_complete_failed_release() {
    let mut state = state();
    populate(&mut state, 1);
    let effects = on_event(&mut state, Event::ReleaseAllForDiskRemoval);
    let op = effects
        .iter()
        .find_map(|e| match e {
            Effect::EnsureDurable { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let effects = on_event(&mut state, Event::DurabilityChecked { op, result: Ok(()) });
    assert!(!effects
        .iter()
        .any(|e| matches!(e, Effect::ReleaseOwner { .. })));
    assert_eq!(state.occupied(), 1);
    assert_eq!(state.drain_progress(), 0);
    let op = effects
        .iter()
        .find_map(|e| match e {
            Effect::StopRuntime { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let effects = on_event(&mut state, Event::RuntimeStopped { op });
    assert_eq!(state.occupied(), 0);
    assert_eq!(state.releasing(), 1);
    assert_eq!(state.drain_progress(), 0);
    let op = effects
        .iter()
        .find_map(|e| match e {
            Effect::ReleaseOwner { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let effects = on_event(
        &mut state,
        Event::OwnerReleased {
            op,
            result: Err(Failure::Ambiguous),
        },
    );
    assert_eq!(state.drain_progress(), 0);
    assert_eq!(state.releasing(), 1);
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::ReleaseOwner { .. })));
    assert!(!effects
        .iter()
        .any(|e| matches!(e, Effect::AdoptReleased { .. })));
    state.validate().unwrap();
}
