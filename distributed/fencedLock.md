### Shortcomings of current approach
Currently, we are using a distributed lock implementation of Hazelcast which does not guarantee mutual exclusive access as it is impacted by split-brain. All operations that rely on `ODistributedAbstractPlugin.executeInDistributedDatabaseLock` and `OHazelcastLockManager.acquireExclusiveLock` are impacted by this problem. 

Hazelcast 3.12 and later provide a 'CP subsystem' that has a `FencedLock` implementation that allows using a fencing technique to ensure exclusive access. In fencing, a client first acquires a lock and for each request that is sent inside this lock (e.g. to another node/service), a fencing token is also sent. The service accepts only requests with fencing tokens that are monotonically increasing. 

Even if we can use the Hazelcast `FencedLock` properly, it does not address all cases.

Current locks used in Hazelcast:
- `DBNAME_LOCK`: For each database there is a lock with the same name which is used to ensure that operations triggered by different master servers for that DB do not run in parallel.

Sometimes, the `DBNAME_LOCK` is acquired more than once. E.g., `loadLocalDatabases` is called while having the lock which internally calls `reassignClusterOwnership` which also acquires the same lock.

A lot of metadata is stored locally on each server and distributed locking and Hazelcast event listeners are used on each node to locally maintain a copy of this metadata (`OHazelcastDistributedMap`).

`getAvailableNodeNames` simply reads node status from the HC map! Which could be easily outdated!

**TODO**: find relation of the following operations with this metadata kept on OHazelcastDistributedMap:
*  getDatabaseStatus

Following operations rely on the distributed locking (and `DBNAME_LOCK`) in Hazelcast. The lock is used to ensure these operations are not performed in parallel.

##### reassignClusterOwnership
Called when a node joins or leaves the cluster. The lock is required to ensure that only one node performs the reassignment of clusters or creation of new ones. This operation results in an update to distributed config of a DB which get sent to other nodes by storing it in the MAP and sending an `OUpdateDatabaseConfigurationTask` to all other servers. 

Why is the lock needed? To ensure that only one master will create the new distributed config for that DB. Any master getting the lock afterwards, results in producing the same config for the DB. The new config is saved to the IMap and 'pushed' to other masters which simply replace their local DB config.

Having a raft leader in charge of accepting and calculating config changes might make the distributed lock unnecessary.
  
##### installDatabase
Triggers a full/delta sync.

##### Node joining the HC cluster
Once a node joins the cluster, its databases is also 'merged' with the rest of the cluster!

##### installResponseNewDeltaSync

##### loadLocalDatabases

##### installClustersOfClass

##### installDatabaseOnLocalNode

##### removeNodeFromConfiguration

##### checkNodeInConfiguration/registerNewDatabaseIfNeeded
Once a node loads its local version of a distributed database, it will add itself as one of the nodes/masters for that database.

##### drop database

##### createClusters (for a database)

##### OSchemaDistributed.acquireSchemaWriteLock 
also acquires the lock.

---
Following need to be checked if we move away from Map+Lock:
* OClusterHealthChecker
* DB_STATUS