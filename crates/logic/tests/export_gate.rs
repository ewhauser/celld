//! The change exporter's barrier on the output gate, driven through
//! `on_event` with a shell that answers every activation effect at once.

use celld_logic::{
    on_event, CasOutcome, Channel, Config, Effect, Event, Failure, Phase, ProofSource,
    RequestError, RestoreOutcome, State,
};

const CELL: &str = "cell";

fn config() -> Config {
    Config {
        max_resident: 8,
        max_activations: 8,
        max_evictions: 1,
        max_releases: 1,
        max_outbound_websockets: 1,
        ownership_on_evict: Default::default(),
        require_node_lease: false,
        peer_protocol: 1,
        operation_deadline_ms: None,
        owner_log_recovery_backoff_ms: 0,
        owner_log_recovery_attempts: 1,
        alarm_resident_ms: 0,
        idle_evict_ms: None,
        pressure: Default::default(),
    }
}

/// Feed `event`, answer every activation effect with success, and return the
/// effects nothing answered.
fn drive(state: &mut State, event: Event) -> Vec<Effect> {
    let mut queue = vec![event];
    let mut left = Vec::new();
    while let Some(event) = queue.pop() {
        let effects = on_event(state, event);
        state.validate().expect("the core stays consistent");
        for effect in effects {
            match effect {
                Effect::ReadOwner { op, .. } => queue.push(Event::OwnerRead {
                    op,
                    now_ms: 0,
                    result: Ok(None),
                }),
                Effect::CasOwner { op, .. } => queue.push(Event::OwnerCasCompleted {
                    op,
                    result: Ok(CasOutcome::Applied),
                }),
                Effect::Restore { op, .. } => queue.push(Event::RestoreCompleted {
                    op,
                    result: Ok(RestoreOutcome {
                        restored: false,
                        alarm: None,
                    }),
                }),
                Effect::StartRuntime { op, .. } => queue.push(Event::RuntimeStarted {
                    op,
                    isolate: None,
                    generation: 0,
                    result: Ok(()),
                }),
                Effect::Publish { op, .. } => queue.push(Event::Published { op, result: Ok(()) }),
                Effect::ScheduleTimer { .. } | Effect::ReconcileWakeEntry { .. } => {}
                other => left.push(other),
            }
        }
    }
    left
}

fn resident_epoch(state: &State) -> Option<u64> {
    match state.phase(CELL) {
        Some(Phase::Resident { epoch }) => Some(*epoch),
        _ => None,
    }
}

/// A node with `CELL` resident at its epoch, and no request on it.
fn resident() -> (State, u64) {
    let mut state = State::new("node", config());
    let effects = drive(
        &mut state,
        Event::Request {
            request: 1,
            cell: CELL.to_string(),
        },
    );
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::Complete {
                request: 1,
                result: Ok(_)
            }
        )),
        "{effects:?}"
    );
    drive(&mut state, Event::ActivityFinished { request: 1 });
    let epoch = resident_epoch(&state).expect("the request left the cell resident");
    (state, epoch)
}

fn ticket(state: &mut State, epoch: u64, position: u64, ticket: u64) -> Vec<Effect> {
    drive(
        state,
        Event::ExportTicket {
            cell: CELL.to_string(),
            epoch,
            position,
            ticket,
        },
    )
}

fn await_op(effects: &[Effect]) -> u64 {
    match effects {
        [Effect::AwaitDurable { op, .. }] => *op,
        other => panic!("expected one proof, got {other:?}"),
    }
}

#[test]
fn a_fleet_proof_settles_the_ticket() {
    let (mut state, epoch) = resident();
    let op = await_op(&ticket(&mut state, epoch, 5, 7));
    let effects = drive(
        &mut state,
        Event::DurableReached {
            op,
            result: Ok(5),
            source: ProofSource::Fleet,
        },
    );
    assert_eq!(
        effects,
        [Effect::ExportProven {
            cell: CELL.to_string(),
            epoch,
            ticket: 7,
            result: Ok(()),
        }]
    );
}

#[test]
fn a_bucket_proof_waits_for_the_ownership_read() {
    let (mut state, epoch) = resident();
    let op = await_op(&ticket(&mut state, epoch, 5, 7));
    let effects = drive(
        &mut state,
        Event::DurableReached {
            op,
            result: Ok(6),
            source: ProofSource::Bucket,
        },
    );
    assert!(
        matches!(effects[..], [Effect::VerifyOwnership { .. }]),
        "{effects:?}"
    );
    let effects = drive(
        &mut state,
        Event::OwnershipVerified {
            op,
            result: Err(Failure::Definite),
        },
    );
    assert_eq!(
        effects,
        [Effect::ExportProven {
            cell: CELL.to_string(),
            epoch,
            ticket: 7,
            result: Err(RequestError::DurabilityUnproven),
        }]
    );
    // The exporter's failed proof leaves the cell serving.
    assert_eq!(resident_epoch(&state), Some(epoch));
}

#[test]
fn a_short_proof_fails_the_ticket_without_resetting_the_cell() {
    let (mut state, epoch) = resident();
    let op = await_op(&ticket(&mut state, epoch, 5, 7));
    let effects = drive(
        &mut state,
        Event::DurableReached {
            op,
            result: Ok(4),
            source: ProofSource::Fleet,
        },
    );
    assert!(matches!(
        effects[..],
        [Effect::ExportProven {
            ticket: 7,
            result: Err(RequestError::DurabilityUnproven),
            ..
        }]
    ));
    assert_eq!(resident_epoch(&state), Some(epoch));
}

#[test]
fn a_ticket_for_another_epoch_or_a_fenced_node_is_refused() {
    let (mut state, epoch) = resident();
    assert!(matches!(
        ticket(&mut state, epoch + 1, 5, 7)[..],
        [Effect::ExportProven {
            result: Err(RequestError::DurabilityUnproven),
            ..
        }]
    ));
    let op = await_op(&ticket(&mut state, epoch, 5, 8));
    let effects = drive(&mut state, Event::NodeFenced);
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::ExportProven {
                ticket: 8,
                result: Err(RequestError::NodeFenced),
                ..
            }
        )),
        "{effects:?}"
    );
    // A proof that lands after the fence is ignored.
    let late = drive(
        &mut state,
        Event::DurableReached {
            op,
            result: Ok(5),
            source: ProofSource::Fleet,
        },
    );
    assert!(late.is_empty(), "{late:?}");
    assert!(matches!(
        ticket(&mut state, epoch, 5, 9)[..],
        [Effect::ExportProven {
            result: Err(RequestError::NodeFenced),
            ..
        }]
    ));
}

#[test]
fn a_reader_does_not_wait_on_the_exporter() {
    let (mut state, epoch) = resident();
    await_op(&ticket(&mut state, epoch, 5, 7));
    drive(
        &mut state,
        Event::Request {
            request: 2,
            cell: CELL.to_string(),
        },
    );
    let effects = drive(
        &mut state,
        Event::Output {
            request: 2,
            channel: Channel::Response,
            position: None,
            observed: None,
            epoch: None,
        },
    );
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::Release {
                request: 2,
                result: Ok(()),
                ..
            }
        )),
        "{effects:?}"
    );
}

/// Open `request` on `CELL` and gate its write at `position`. Returns the
/// barrier's proof op.
fn write(state: &mut State, request: u64, position: u64) -> u64 {
    drive(
        state,
        Event::Request {
            request,
            cell: CELL.to_string(),
        },
    );
    await_op(&drive(
        state,
        Event::Output {
            request,
            channel: Channel::Response,
            position: Some(position),
            observed: None,
            epoch: None,
        },
    ))
}

fn proven(state: &mut State, op: u64, durable: u64, source: ProofSource) -> Vec<Effect> {
    drive(
        state,
        Event::DurableReached {
            op,
            result: Ok(durable),
            source,
        },
    )
}

fn owned(state: &mut State, op: u64) -> Vec<Effect> {
    drive(state, Event::OwnershipVerified { op, result: Ok(()) })
}

fn export_proven(epoch: u64, ticket: u64) -> Effect {
    Effect::ExportProven {
        cell: CELL.to_string(),
        epoch,
        ticket,
        result: Ok(()),
    }
}

fn released(request: u64) -> Effect {
    Effect::Release {
        request,
        channel: Channel::Response,
        result: Ok(()),
    }
}

/// The ownership reads asked in `effects`.
fn reads(effects: &[Effect]) -> usize {
    effects
        .iter()
        .filter(|effect| matches!(effect, Effect::VerifyOwnership { .. }))
        .count()
}

#[test]
fn a_ticket_rides_the_read_of_a_write_still_proving() {
    let (mut state, epoch) = resident();
    let op = write(&mut state, 2, 5);
    // The write's barrier covers the commit, so the ticket asks for nothing.
    assert_eq!(ticket(&mut state, epoch, 5, 7), []);
    let effects = proven(&mut state, op, 5, ProofSource::Bucket);
    assert!(
        matches!(effects[..], [Effect::VerifyOwnership { op: read, .. }] if read == op),
        "{effects:?}"
    );
    let effects = owned(&mut state, op);
    assert!(effects.contains(&released(2)), "{effects:?}");
    assert!(effects.contains(&export_proven(epoch, 7)), "{effects:?}");
}

#[test]
fn a_ticket_above_the_write_or_after_its_read_proves_itself() {
    let (mut state, epoch) = resident();
    write(&mut state, 2, 4);
    // The write's proof does not cover position 5.
    await_op(&ticket(&mut state, epoch, 5, 7));

    let (mut state, epoch) = resident();
    let op = write(&mut state, 2, 5);
    assert_eq!(reads(&proven(&mut state, op, 5, ProofSource::Bucket)), 1);
    // The write's read was asked before the ticket arrived.
    await_op(&ticket(&mut state, epoch, 5, 7));
}

#[test]
fn a_write_reading_takes_an_export_barrier_at_or_below_it() {
    let (mut state, epoch) = resident();
    let export = await_op(&ticket(&mut state, epoch, 5, 7));
    let op = write(&mut state, 2, 5);
    // One read for both.
    let effects = proven(&mut state, op, 5, ProofSource::Bucket);
    assert!(
        matches!(effects[..], [Effect::VerifyOwnership { op: read, .. }] if read == op),
        "{effects:?}"
    );
    let effects = owned(&mut state, op);
    assert!(effects.contains(&released(2)), "{effects:?}");
    assert!(effects.contains(&export_proven(epoch, 7)), "{effects:?}");
    // The export barrier's own proof landing later settles nothing again.
    assert_eq!(proven(&mut state, export, 5, ProofSource::Bucket), []);
}

#[test]
fn an_export_proof_hands_its_ticket_to_a_write_still_proving() {
    let (mut state, epoch) = resident();
    let export = await_op(&ticket(&mut state, epoch, 5, 7));
    // A write below the ticket: its proof does not cover the ticket, but its
    // read comes after the ticket's own proof.
    let op = write(&mut state, 2, 3);
    assert_eq!(proven(&mut state, export, 5, ProofSource::Bucket), []);
    assert_eq!(reads(&proven(&mut state, op, 3, ProofSource::Bucket)), 1);
    let effects = owned(&mut state, op);
    assert!(effects.contains(&released(2)), "{effects:?}");
    assert!(effects.contains(&export_proven(epoch, 7)), "{effects:?}");
}

#[test]
fn an_export_proof_reads_for_itself_when_no_write_is_proving() {
    let (mut state, epoch) = resident();
    let export = await_op(&ticket(&mut state, epoch, 5, 7));
    let op = write(&mut state, 2, 3);
    // The write reads first; the ticket is above it, so it is not taken.
    assert_eq!(reads(&proven(&mut state, op, 3, ProofSource::Bucket)), 1);
    let effects = proven(&mut state, export, 5, ProofSource::Bucket);
    assert!(
        matches!(effects[..], [Effect::VerifyOwnership { op: read, .. }] if read == export),
        "{effects:?}"
    );
    assert_eq!(owned(&mut state, export), [export_proven(epoch, 7)]);
}

#[test]
fn a_failed_read_fails_the_rider_with_the_write() {
    let (mut state, epoch) = resident();
    let op = write(&mut state, 2, 5);
    assert_eq!(ticket(&mut state, epoch, 5, 7), []);
    proven(&mut state, op, 5, ProofSource::Bucket);
    let effects = drive(
        &mut state,
        Event::OwnershipVerified {
            op,
            result: Err(Failure::Definite),
        },
    );
    assert!(
        effects.contains(&Effect::ExportProven {
            cell: CELL.to_string(),
            epoch,
            ticket: 7,
            result: Err(RequestError::DurabilityUnproven),
        }),
        "{effects:?}"
    );
}

#[test]
fn a_fleet_proof_asks_its_riders_again() {
    let (mut state, epoch) = resident();
    let op = write(&mut state, 2, 5);
    assert_eq!(ticket(&mut state, epoch, 5, 7), []);
    // A fleet acknowledgement can predate the ticket, so the ticket is not
    // settled by it.
    let effects = proven(&mut state, op, 5, ProofSource::Fleet);
    assert!(effects.contains(&released(2)), "{effects:?}");
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::ExportProven { .. })),
        "{effects:?}"
    );
    let reasked = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::AwaitDurable {
                op, position: 5, ..
            } => Some(*op),
            _ => None,
        })
        .expect("the rider proves itself");
    assert_eq!(
        proven(&mut state, reasked, 5, ProofSource::Fleet),
        [export_proven(epoch, 7)]
    );
}

#[test]
fn a_fence_fails_the_riders() {
    let (mut state, epoch) = resident();
    write(&mut state, 2, 5);
    assert_eq!(ticket(&mut state, epoch, 5, 7), []);
    let effects = drive(&mut state, Event::NodeFenced);
    assert!(
        effects.contains(&Effect::ExportProven {
            cell: CELL.to_string(),
            epoch,
            ticket: 7,
            result: Err(RequestError::NodeFenced),
        }),
        "{effects:?}"
    );
}

#[test]
fn a_cancelled_output_asks_its_riders_again() {
    let (mut state, epoch) = resident();
    write(&mut state, 2, 5);
    assert_eq!(ticket(&mut state, epoch, 5, 7), []);
    let effects = drive(&mut state, Event::ReleaseAll);
    assert!(
        effects.contains(&Effect::Release {
            request: 2,
            channel: Channel::Response,
            result: Err(RequestError::DurabilityUnproven),
        }),
        "{effects:?}"
    );
    let reasked = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::AwaitDurable {
                op, position: 5, ..
            } => Some(*op),
            _ => None,
        })
        .expect("the rider proves itself");
    assert_eq!(
        reads(&proven(&mut state, reasked, 5, ProofSource::Bucket)),
        1
    );
    assert_eq!(owned(&mut state, reasked), [export_proven(epoch, 7)]);
}

#[test]
fn a_relayed_ticket_rides_the_roots_export_barrier() {
    let (mut state, epoch) = resident();
    let export = await_op(&ticket(&mut state, epoch, 5, 7));
    // A facet's relay asks at position 0, after the facet's own proof.
    assert_eq!(ticket(&mut state, epoch, 0, 8), []);
    assert_eq!(
        reads(&proven(&mut state, export, 5, ProofSource::Bucket)),
        1
    );
    let effects = owned(&mut state, export);
    assert_eq!(effects.len(), 2, "{effects:?}");
    assert!(effects.contains(&export_proven(epoch, 7)), "{effects:?}");
    assert!(effects.contains(&export_proven(epoch, 8)), "{effects:?}");
}
