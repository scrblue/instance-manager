use super::*;

use std::collections::HashSet;
use tempdir::TempDir;

fn get_state_manager() -> StateManager {
    let dir = TempDir::new("ppcim").unwrap();

    let mut dist_path: PathBuf = dir.path().into();
    dist_path.push("dist");

    let mut log_path: PathBuf = dir.path().into();
    log_path.push("log");

    StateManager::new(0, dist_path, log_path, dir.path().into()).unwrap()
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
    let sm = get_state_manager();

    assert_eq!(
        sm.get_membership_config().await.unwrap(),
        MembershipConfig::new_initial(0)
    );
}

#[tokio::test]
async fn get_membership_config_cc_test() {
    let sm = get_state_manager();

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
    let sm = get_state_manager();

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
    let sm = get_state_manager();

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
