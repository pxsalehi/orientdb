### Shortcomings of current approach
Currently, we are using a distributed lock implementation of Hazelcast which does not guarantee mutual exclusive access as it is impacted by split-brain. All operations that rely on `ODistributedAbstractPlugin.executeInDistributedDatabaseLock` and `OHazelcastLockManager.acquireExclusiveLock` are impacted by this problem. 

Hazelcast 3.12 and later provide a 'CP subsystem' that has a `FencedLock` implementation that allows using a fencing technique to ensure exclusive access. In fencing, a client first acquires a lock and for each request that is sent inside this lock (e.g. to another node/service), a fencing token is also sent. The service accepts only requests with fencing tokens that are monotonically increasing. 

Even if we can use the Hazelcast `FencedLock` properly, it does not address all cases.

Current locks used in Hazelcast:
- `DBNAME_LOCK`: For each database there is a lock with the same name which is used to ensure that operations triggered by different master servers for that DB do not run in parallel.

Sometimes, the `DBNAME_LOCK` is acquired more than once. E.g., `loadLocalDatabases` is called while having the lock which internally calls `reassignClusterOwnership` which also acquires the same lock.

A lot of metadata is stored locally on each server and distributed locking and Hazelcast event listeners are used on each node to locally maintain a copy of this metadata (`OHazelcastDistributedMap`).

`getAvailableNodeNames` simply reads node status from the HC map! Which could be easily outdated!

Following operations rely on the distributed locking (and `DBNAME_LOCK`) in Hazelcast:

1. `reassignClusterOwnership`: Called when a node joins or leaves the cluster. The lock is required to ensure that only one node performs the reassignment of clusters or creation of new ones. This operation results in an update to distributed config of a DB which get sent to other nodes by storing it in the MAP and sending an `OUpdateDatabaseConfigurationTask` to all other servers. **Why is the lock needed**?  
  
2. `installDatabase`

3. Node joining the HC cluster: Once a node joins the cluster, its databases is also 'merged' with the rest of the cluster!

4. `installResponseNewDeltaSync`

5. `loadLocalDatabases`

6. `installClustersOfClass`

7. `installDatabaseOnLocalNode`

8. `removeNodeFromConfiguration`

9. `checkNodeInConfiguration`/`registerNewDatabaseIfNeeded`: Once a node loads its local version of a distributed database, it will add itself as one of the nodes/masters for that database.

10. drop database

11. `createClusters` (for a database)

12. `OSchemaDistributed.acquireSchemaWriteLock` also acquires the lock.

