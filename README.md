## SIMPLE RAFT

This is an extremely simple implementation of raft for learning purpose.

### TODO

- Handle state transition in a more efficient way to handle case where the node receive a greater term and need to change the state IMMEDIATELY.
- Check race condition bugs that can happen?
- Implement atomicity for upgrading term, persistent state for nodes.