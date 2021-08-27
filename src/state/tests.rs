use super::*;

use std::collections::HashSet;
use tempdir::TempDir;

fn get_state_manager() -> (StateManager, TempDir) {
    let dir = TempDir::new("ppcim").unwrap();

    let mut dist_path: PathBuf = dir.path().into();
    dist_path.push("dist");

    let mut log_path: PathBuf = dir.path().into();
    log_path.push("log");

    (StateManager::new(0, dist_path, log_path, dir.path().into()).unwrap(), dir)
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
