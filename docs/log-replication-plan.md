Multi-Phase Plan: Consolidate Heartbeats into AppendEntries and Implement Leader Replication

Overview
We will remove the dedicated LeaderAssert RPC and rely on AppendEntries for both heartbeats and log replication, as per the Raft spec. This will require proto changes, server/client refactors, and leader-side replication state tracking. The work is broken into phases to keep risk low and make testing/validation clearer.

Phase 1: Protocol and server consolidation
Goal: Make AppendEntries the single RPC for heartbeats; remove LeaderAssert.
Steps:
- Update proto/consensus.proto:
  - Remove LeaderAssertRequest/LeaderAssertResponse messages.
  - Remove LeaderAssert RPC from ConsensusService.
  - Keep AppendEntries as-is (entries empty == heartbeat).
- Regenerate gRPC code by running cargo build (required after proto changes).
- Update server implementation in src/services/consensus.rs:
  - Remove leader_assert handler.
  - Ensure append_entries resets election timer and performs term/role checks (already largely in place).
  - Keep heartbeat path (entries empty) as a success case.
Acceptance:
- No references to LeaderAssert remain in code.
- AppendEntries handles heartbeat without entries.

Phase 2: Leader heartbeat via AppendEntries
Goal: Use AppendEntries for periodic heartbeats.
Steps:
- Update src/cluster/consensus.rs:
  - Replace LeaderAssertRequest usage with AppendEntriesRequest for heartbeat rounds.
  - Populate request fields: term, leader_id, prev_log_idx, prev_log_term, leader_commit, entires = [].
  - If any response returns a higher term, step down and persist meta.
Acceptance:
- Leader heartbeat loop sends AppendEntries and steps down on higher term.

Phase 3: Leader-side replication state
Goal: Track per-peer replication indices (next_index, match_index).
Steps:
- Extend peer state (likely in src/cluster/mod.rs Peer or a leader-only map in src/cluster/consensus.rs):
  - next_index: u32 (next log index to send to the peer)
  - match_index: u32 (highest log index known to be replicated on the peer)
  - Initialize next_index = local_last_index + 1 on leadership.
  - Initialize match_index = 0.
- Ensure peer state is accessible from the leader heartbeat/replication task.
Acceptance:
- Peer replication state is created and initialized when leader starts.

Phase 4: AppendEntries replication send path
Goal: Send actual log entries based on next_index and handle conflicts.
Steps:
- For each peer on each heartbeat tick:
  - Determine prev_log_idx = next_index - 1.
  - Determine prev_log_term from local log (handle 0 index).
  - Load log entries starting at next_index (batch size optional).
  - Build AppendEntriesRequest with entries.
- On AppendEntriesResponse:
  - If success: advance match_index and next_index.
  - If conflict_term provided: set next_index to first index of that term (or conflict_index) and retry on next tick.
  - If conflict_term missing: set next_index to conflict_index.
  - If response term > current term: step down immediately.
Acceptance:
- next_index/match_index update correctly based on responses.
- Conflicts adjust next_index as per response fields.

Phase 5: Leader commit advancement
Goal: Move leader_commit forward based on majority replication.
Steps:
- Compute the highest log index N such that:
  - N > current leader_commit
  - A majority of match_index values >= N
  - The log term at N == current term (Raft safety rule)
- Update leader_commit in leader state and include it in subsequent AppendEntries.
Acceptance:
- leader_commit advances only when quorum is achieved for current term.

Phase 6: Validate and follow-ups
Goal: Ensure build and baseline runtime behavior.
Steps:
- Run cargo check (if no proto changes) or cargo build (if proto changed).
- Optional: add tracing around replication decisions and conflict handling for debugging.

Notes and assumptions
- Log read APIs currently expose: get_last_log_index(), get_last_log_term(), get_entry_term(idx), find_first_index_of_term(term).
- There is no direct log-entry range fetch helper yet; we may need to add a helper to read entries by index to build AppendEntries entries.
- AppendEntriesRequest field name is currently "entires" in proto; we will keep it to avoid breaking generated code unless renaming is explicitly desired.
