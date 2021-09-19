use super::*;

use std::collections::HashSet;
use tempdir::TempDir;

fn get_state_manager() -> (StateManager, TempDir) {
    let dir = TempDir::new("ppcim").unwrap();

    let mut dist_path: PathBuf = dir.path().into();
    dist_path.push("dist");

    let mut log_path: PathBuf = dir.path().into();
    log_path.push("log");

    (
        StateManager::new(0, dist_path, log_path, dir.path().into()).unwrap(),
        dir,
    )
}

#[test]
fn state_manager_new_test() {
    let dir = TempDir::new("ppcim").unwrap();

    let mut dist_path: PathBuf = dir.path().into();
    dist_path.push("dist");

    let mut log_path: PathBuf = dir.path().into();
    log_path.push("log");

    StateManager::new(0, dist_path, log_path, dir.path().into()).unwrap();
}

// get_membership_config tests

#[tokio::test]
async fn get_membership_config_new_test() {
    let (sm, _dir) = get_state_manager();

    assert_eq!(
        sm.get_membership_config().await.unwrap(),
        MembershipConfig::new_initial(0)
    );

    let transaction = sm.distributed_state.transaction().unwrap();
    assert_eq!(transaction.get_vertex_count().unwrap(), 1);
}

#[tokio::test]
async fn get_membership_config_cc_test() {
    let (sm, _dir) = get_state_manager();

    let mc = MembershipConfig {
        members: HashSet::new(),
        members_after_consensus: None,
    };

    sm.raft_log_db
        .put(
            1u64.to_le_bytes(),
            bincode::serialize(&Entry::<RaftRequest> {
                term: 0,
                index: 1,
                payload: EntryPayload::ConfigChange(EntryConfigChange {
                    membership: mc.clone(),
                }),
            })
            .unwrap(),
        )
        .unwrap();

    assert_eq!(sm.get_membership_config().await.unwrap(), mc);
}

#[tokio::test]
async fn get_membership_config_sp_test() {
    let (sm, _dir) = get_state_manager();

    let mc = MembershipConfig {
        members: HashSet::new(),
        members_after_consensus: None,
    };

    sm.raft_log_db
        .put(
            1u64.to_le_bytes(),
            bincode::serialize(&Entry::<RaftRequest> {
                term: 0,
                index: 1,
                payload: EntryPayload::SnapshotPointer(EntrySnapshotPointer {
                    id: "".to_owned(),
                    membership: mc.clone(),
                }),
            })
            .unwrap(),
        )
        .unwrap();

    assert_eq!(sm.get_membership_config().await.unwrap(), mc);
}

#[tokio::test]
async fn get_membership_config_ordering_test() {
    // Given both a ConfigChange and a SnapshotPointer in the log DB, return the more recent one
    let (sm, _dir) = get_state_manager();

    let mc1 = MembershipConfig {
        members: HashSet::new(),
        members_after_consensus: Some(HashSet::new()),
    };

    let mc2 = MembershipConfig {
        members: HashSet::new(),
        members_after_consensus: None,
    };

    sm.raft_log_db
        .put(
            1u64.to_le_bytes(),
            bincode::serialize(&Entry::<RaftRequest> {
                term: 0,
                index: 1,
                payload: EntryPayload::SnapshotPointer(EntrySnapshotPointer {
                    id: "".to_owned(),
                    membership: mc1,
                }),
            })
            .unwrap(),
        )
        .unwrap();

    sm.raft_log_db
        .put(
            2u64.to_le_bytes(),
            bincode::serialize(&Entry::<RaftRequest> {
                term: 0,
                index: 2,
                payload: EntryPayload::ConfigChange(EntryConfigChange {
                    membership: mc2.clone(),
                }),
            })
            .unwrap(),
        )
        .unwrap();

    assert_eq!(sm.get_membership_config().await.unwrap(), mc2);
}

// get_initial_state tests

#[tokio::test]
async fn get_initial_state_new_test() {
    let (sm, _dir) = get_state_manager();

    let is_expected = InitialState::new_initial(0);
    let is_actual = sm.get_initial_state().await.unwrap();

    assert_eq!(is_expected.last_log_index, is_actual.last_log_index);
    assert_eq!(is_expected.last_log_term, is_actual.last_log_term);
    assert_eq!(is_expected.last_applied_log, is_actual.last_applied_log);
    assert_eq!(is_expected.hard_state, is_actual.hard_state);
    assert_eq!(is_expected.membership, is_actual.membership);
}

#[tokio::test]
async fn get_initial_state_cc_test() {
    let (sm, _dir) = get_state_manager();

    let mc = MembershipConfig {
        members: HashSet::new(),
        members_after_consensus: None,
    };
    let hs = HardState {
        current_term: 1,
        voted_for: None,
    };

    let transaction = sm.distributed_state.transaction().unwrap();
    assert_eq!(transaction.get_vertex_count().unwrap(), 1);

    sm.save_hard_state(&hs).await.unwrap();

    sm.raft_log_db
        .put(
            1u64.to_le_bytes(),
            bincode::serialize(&Entry::<RaftRequest> {
                term: 1,
                index: 1,
                payload: EntryPayload::ConfigChange(EntryConfigChange {
                    membership: mc.clone(),
                }),
            })
            .unwrap(),
        )
        .unwrap();

    let is_actual = sm.get_initial_state().await.unwrap();

    assert_eq!(1, is_actual.last_log_index);
    assert_eq!(1, is_actual.last_log_term);
    assert_eq!(0, is_actual.last_applied_log);
    assert_eq!(hs, is_actual.hard_state);
    assert_eq!(mc, is_actual.membership);
}

// save_hard_state test

#[tokio::test]
async fn save_hard_state_test() {
    let (sm, _dir) = get_state_manager();

    // Test initial
    let hs_expexcted = HardState {
        current_term: 0,
        voted_for: None,
    };
    assert_eq!(hs_expexcted, sm.get_hard_state().await.unwrap());

    // Save 1
    let hs_expexcted = HardState {
        current_term: 1,
        voted_for: None,
    };
    sm.save_hard_state(&hs_expexcted).await.unwrap();
    assert_eq!(hs_expexcted, sm.get_hard_state().await.unwrap());

    // Save 2
    let hs_expexcted = HardState {
        current_term: 2,
        voted_for: Some(1),
    };
    sm.save_hard_state(&hs_expexcted).await.unwrap();
    assert_eq!(hs_expexcted, sm.get_hard_state().await.unwrap());
}

// get_log_entries, delete_logs_from, append_entry_to_log, and replicate_to_log test

#[tokio::test]
async fn log_operations_test() {
    let (sm, _dir) = get_state_manager();

    let le1 = Entry {
        term: 1,
        index: 1,
        payload: EntryPayload::Normal(EntryNormal {
            data: RaftRequest {
                client: uuid::Uuid::nil(),
                serial: 0,
                payload: RaftRequestPayload::ConsoleNetworkRequest(
                    ConsoleNetworkRequest::Shutdown(0),
                ),
            },
        }),
    };
    let le2 = Entry {
        term: 1,
        index: 2,
        payload: EntryPayload::Normal(EntryNormal {
            data: RaftRequest {
                client: uuid::Uuid::nil(),
                serial: 0,
                payload: RaftRequestPayload::ConsoleNetworkRequest(
                    ConsoleNetworkRequest::Shutdown(1),
                ),
            },
        }),
    };
    let le3 = Entry {
        term: 1,
        index: 3,
        payload: EntryPayload::Normal(EntryNormal {
            data: RaftRequest {
                client: uuid::Uuid::nil(),
                serial: 0,
                payload: RaftRequestPayload::ConsoleNetworkRequest(
                    ConsoleNetworkRequest::Shutdown(2),
                ),
            },
        }),
    };

    assert_eq!(sm.get_log_entries(1, 4).await.unwrap().len(), 0);

    sm.append_entry_to_log(&le1).await.unwrap();
    let les = sm.get_log_entries(1, 4).await.unwrap();
    assert_eq!(les.len(), 1);
    assert_eq!(les[0], le1);

    let les_expected = [le1, le2, le3];
    sm.replicate_to_log(&les_expected[1..]).await.unwrap();
    let les = sm.get_log_entries(1, 4).await.unwrap();
    assert_eq!(les.as_slice(), les_expected);

    let les = sm.get_log_entries(1, 3).await.unwrap();
    assert_eq!(les.as_slice(), &les_expected[..2]);

    let les = sm.get_log_entries(2, 3).await.unwrap();
    assert_eq!(les.as_slice(), &les_expected[1..2]);

    sm.delete_logs_from(3, Some(4)).await.unwrap();
    let les = sm.get_log_entries(1, 4).await.unwrap();
    assert_eq!(les.as_slice(), &les_expected[0..2]);

    sm.delete_logs_from(1, None).await.unwrap();
    let les = sm.get_log_entries(1, 4).await.unwrap();
    assert_eq!(les.len(), 0);
}

// TODO: apply_entry_to_state_machine and replicate_to_state_machine testing

// Snapshot test

#[tokio::test]
async fn snapshot_tests() {
    let (sm, _dir) = get_state_manager();

    let le1 = Entry {
        term: 1,
        index: 1,
        payload: EntryPayload::ConfigChange(EntryConfigChange {
            membership: MembershipConfig::new_initial(0),
        }),
    };
    let le2 = Entry {
        term: 1,
        index: 2,
        payload: EntryPayload::Normal(EntryNormal {
            data: RaftRequest {
                client: uuid::Uuid::nil(),
                serial: 0,
                payload: RaftRequestPayload::ConsoleNetworkRequest(
                    ConsoleNetworkRequest::Shutdown(1),
                ),
            },
        }),
    };
    let le3 = Entry {
        term: 1,
        index: 3,
        payload: EntryPayload::Normal(EntryNormal {
            data: RaftRequest {
                client: uuid::Uuid::nil(),
                serial: 0,
                payload: RaftRequestPayload::ConsoleNetworkRequest(
                    ConsoleNetworkRequest::Shutdown(2),
                ),
            },
        }),
    };

    let uuid = sm
        .distributed_state
        .transaction()
        .unwrap()
        .create_vertex_from_type(Type::new("Something").unwrap())
        .unwrap();

    let les = [le1, le2, le3];
    sm.replicate_to_log(&les[..]).await.unwrap();
    sm.apply_entry_to_state_machine(
        &2,
        &RaftRequest {
            client: uuid::Uuid::nil(),
            serial: 0,
            payload: RaftRequestPayload::ConsoleNetworkRequest(ConsoleNetworkRequest::Shutdown(1)),
        },
    )
    .await
    .unwrap();

    let mut file = sm.do_log_compaction().await.unwrap();
    assert_eq!(sm.get_log_entries(1, 4).await.unwrap().len(), 2);

    let (sm2, _dir2) = get_state_manager();

    let (filename, mut file2) = sm2.create_snapshot().await.unwrap();

    let mut buffer = Vec::new();
    file.snapshot.read_to_end(&mut buffer).await.unwrap();
    file2.write_all(&buffer).await.unwrap();

    sm2.finalize_snapshot_installation(3, 1, Some(3), filename, file2)
        .await
        .unwrap();

    assert_eq!(sm2.get_log_entries(1, 4).await.unwrap().len(), 1);
    assert!(sm2
        .distributed_state
        .transaction()
        .unwrap()
        .get_vertices(SpecificVertexQuery::single(uuid))
        .unwrap()
        .pop()
        .is_some())
}
