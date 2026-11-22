# PeerNet

PeerNet is a peer-to-peer file replication network for the local network.
There is no central server: nodes find each other, share files, agree on which files are genuine, and
keep a matching record of everything that has been accepted.

## Features

### Automatic peer discovery
When a node starts, it announces itself on the local network and connects to any other nodes it finds.
No IP addresses or configuration are needed. Connections are kept healthy with a heartbeat, and
dropped peers are handled gracefully.

By default a node uses TCP port 6969 for peer connections and UDP port 5000 for discovery, so allow
both through the firewall.

### Drop-in file sharing
To share a file, place it in the node's `DropZone` folder. The node picks it up, sends it to every
connected peer, and once the network has accepted it, moves it to `DropZone/Published`. If a file
can't be accepted after several attempts, it's moved to `DropZone/Failed` instead.

### Leaderless consensus
A file isn't trusted just because one node sent it. Each node that receives a verified copy publishes
a signed *attestation* of its checksum, and a file is only accepted once a set number of different
nodes agree on it (the *replication factor*). No node is in charge, and nodes never need to know how
large the network is.

### Persistent node identity
Each node has its own cryptographic key, created on first run and kept between restarts. Its node ID
comes from that key, and attestations are signed with it, so other nodes can check who vouched for a file.

### Tamper-evident ledger
Every accepted file is recorded in a ledger that is saved to disk. Each entry is chained to the one
before it by a hash, so altering history breaks the chain.

### Ledger sync and self-repair
Nodes compare ledgers with their peers. A node that was offline catches up by fetching files it
missed from the peers that hold them. On startup, every stored file is checked against the ledger,
and any that are missing or corrupted are re-downloaded from another node.

### Integrity checks on every transfer
Files are checksummed as they're received. Incomplete, oversized or mismatched transfers are discarded
cleanly without affecting other transfers.

### Readable logging
Console output is colour-coded by type of event (setup, network, consensus), with warnings and errors
always highlighted. Log detail can be tuned per area in `appsettings.json`.

### Where things are stored

Next to the application:

| Folder | Contents |
| --- | --- |
| `DropZone/` | Files waiting to be shared (`Published/` and `Failed/` hold the results). |
| `ReceivedFiles/` | Files accepted from the network. |
| `AppData/` | The ledger and this node's identity keys. Keep the private key safe and never share it. |


### TODOs
- Incorporate "groupings" of nodes (potentially protected with a shared certificate to allow connection establishment). 
  This would need either different port configurations, or file directories. Ledgers from disparate groups cannot sync - each ledger item builds a chain from the last item checksum.
- On a wide scale level, every node knows about every other node - this would cause performance issues. 
  This would introduce a few issues. Nodes would need to discover a given subset of other nodes and maintain parity. 
  Similar to the above, to facilitate this the ledger hashing system would have to be decoupled from the chaining. 
- Add some security against file size and accepted filetypes. The current implementation is focused around the assumption that a node joining the network is trusted.