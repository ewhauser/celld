use celld_logic::log_evict::{FollowerHealth, PROBE_FAILURES_TO_DEGRADE};

#[test]
fn a_failed_probe_is_not_a_latency_sample() {
    let mut health = FollowerHealth::default();
    health.append_started("gone", 100);
    assert!(!health.probe_failed("gone", 101));
    assert_eq!(health.sample_count("gone"), 0);
    assert_eq!(health.probe_failures("gone"), 1);
    // The failure closes the outstanding attempt and paces the next probe
    // like a completion would.
    assert!(!health.probe_due("gone", 2_000, 2_000));
    assert!(health.probe_due("gone", 2_102, 2_000));
}

#[test]
fn consecutive_failed_probes_degrade() {
    let mut health = FollowerHealth::default();
    let mut now = 0;
    for failure in 1..=PROBE_FAILURES_TO_DEGRADE {
        health.append_started("gone", now);
        let degrade = health.probe_failed("gone", now + 1);
        assert_eq!(degrade, failure >= PROBE_FAILURES_TO_DEGRADE, "{failure}");
        now += 2_001;
    }
    // Still gone: every further failure keeps asking for the degrade.
    assert!(health.probe_failed("gone", now));
}

#[test]
fn an_answer_resets_the_failure_run() {
    let mut health = FollowerHealth::default();
    for at in 1..PROBE_FAILURES_TO_DEGRADE {
        assert!(!health.probe_failed("blip", u64::from(at)));
    }
    health.append_completed("blip", 10, 1);
    assert_eq!(health.probe_failures("blip"), 0);
    assert_eq!(health.sample_count("blip"), 1);
    assert!(!health.probe_failed("blip", 11));
}

#[test]
fn reset_forgets_the_failure_run() {
    let mut health = FollowerHealth::default();
    for at in 1..PROBE_FAILURES_TO_DEGRADE {
        health.probe_failed("gone", u64::from(at));
    }
    health.reset();
    assert_eq!(health.probe_failures("gone"), 0);
    assert!(health.probe_due("gone", 0, 2_000));
}
