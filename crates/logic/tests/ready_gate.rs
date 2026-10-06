use celld_logic::{
    drain::{fleet_status, FleetUnsettled},
    pressure::{Latches, Load, PressureConfig, SHED_RSS_HARD},
    CapacityPeer,
};

fn peer(node: &str, resident_cells: usize) -> CapacityPeer {
    CapacityPeer {
        node: node.into(),
        addr: String::new(),
        expires_ms: 10,
        peer_protocol: 1,
        sampled_ms: 1,
        owned_cells: Some(resident_cells),
        placement_weight: None,
        bucket_format: None,
        resident_cells,
        host_websockets: 0,
        rss_bytes: 0,
        in_use_bytes: None,
        pressured: false,
        memory_headroom: Some(true),
        restoring: 0,
        paced_handoff: true,
        rebalance_paused: false,
        draining: false,
    }
}

#[test]
fn inactive_file_cache_does_not_consume_rollout_reserve() {
    let config = PressureConfig::from_limits(Some(1 << 30), None);
    let load = Load {
        resident_cells: 10,
        rss_bytes: 470 << 20,
        in_use_bytes: 450 << 20,
        cgroup_working_set_bytes: Some(470 << 20),
        cgroup_current_bytes: Some(1000 << 20),
        container_reserved_bytes: 0,
    };
    assert!(config.has_headroom(load));
    // Local shedding and admission continue to use the complete charge.
    assert_eq!(
        config.classify(load, Latches::default()).1,
        Some(SHED_RSS_HARD)
    );

    let active = Load {
        cgroup_working_set_bytes: Some(850 << 20),
        ..load
    };
    assert!(!config.has_headroom(active));
    let reserved = Load {
        container_reserved_bytes: 350 << 20,
        ..load
    };
    assert!(!config.has_headroom(reserved));
    let missing_working_set = Load {
        cgroup_working_set_bytes: None,
        ..load
    };
    assert!(!config.has_headroom(missing_working_set));
}

#[test]
fn cache_heavy_peer_can_wait_for_a_healthy_paced_successor() {
    let joining = peer("joining", 0);
    let mut incumbent = peer("incumbent", 10);
    incumbent.pressured = true;
    assert_eq!(
        fleet_status(None, "joining", 1, &[joining.clone(), incumbent.clone()], 1),
        Ok(())
    );

    incumbent.memory_headroom = Some(false);
    assert_eq!(
        fleet_status(None, "joining", 1, &[joining.clone(), incumbent.clone()], 1),
        Err(FleetUnsettled::MemoryHeadroom {
            node: "incumbent".into()
        })
    );

    incumbent.memory_headroom = None;
    assert!(matches!(
        fleet_status(None, "joining", 1, &[joining.clone(), incumbent.clone()], 1),
        Err(FleetUnsettled::MemoryHeadroom { .. })
    ));

    incumbent.memory_headroom = Some(true);
    let mut pressured_joining = joining.clone();
    pressured_joining.pressured = true;
    assert!(matches!(
        fleet_status(
            None,
            "joining",
            1,
            &[pressured_joining, incumbent.clone()],
            1
        ),
        Err(FleetUnsettled::MemoryHeadroom { .. })
    ));

    let mut unpaced = joining.clone();
    unpaced.paced_handoff = false;
    assert!(matches!(
        fleet_status(None, "joining", 1, &[unpaced, incumbent.clone()], 1),
        Err(FleetUnsettled::MemoryHeadroom { .. })
    ));

    let mut full = joining;
    full.resident_cells = 10;
    assert!(matches!(
        fleet_status(None, "joining", 1, &[full, incumbent], 1),
        Err(FleetUnsettled::MemoryHeadroom { .. })
    ));
}
