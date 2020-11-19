package com.orientechnologies.orient.server.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.Member;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OUncaughtExceptionHandler;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.OSystemDatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentAbstract;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.cluster.OClusterDBConfig;
import com.orientechnologies.orient.server.distributed.cluster.OClusterMetadataManager;
import com.orientechnologies.orient.server.distributed.impl.ODefaultDistributedStrategy;
import com.orientechnologies.orient.server.distributed.impl.ODistributedAbstractPlugin;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class OHazelcastClusterMetadataManager implements OClusterMetadataManager {
  private final HazelcastInstance hz;
  private OHazelcastDistributedMap hzDistributedMap;
  private final String nodeName;
  private Map<String, OClusterDBConfig> dbConfigs = new ConcurrentHashMap<>();
  private ConcurrentMap<String, Member> activeNodesByName = new ConcurrentHashMap<>();
  private ConcurrentMap<String, String> activeNodesNamesByUuid = new ConcurrentHashMap<>();
  private ConcurrentMap<String, String> activeNodesUuidByName = new ConcurrentHashMap<>();
  private ORemoteServerManager remoteServerManager;
  private ODistributedAbstractPlugin distributedServerManager;
  private ODistributedMessageService messageService;

  private volatile ODistributedServerManager.NODE_STATUS status =
      ODistributedServerManager.NODE_STATUS.OFFLINE;

  private String nodeUuid;
  private ODistributedStrategy distributedStrategy = new ODefaultDistributedStrategy();

  // Includes distributed config and distributed lock management implemented based on Hazelcast.
  public OHazelcastClusterMetadataManager(
      String nodeName,
      HazelcastInstance hzInstance,
      ORemoteServerManager remoteServerManager,
      ODistributedAbstractPlugin distributedServerManager,
      ODistributedMessageService messageService) {
    this.hz = hzInstance;
    this.nodeName = nodeName;
    this.remoteServerManager = remoteServerManager;
    this.hzDistributedMap = new OHazelcastDistributedMap(this, hzInstance);
    this.nodeUuid = hzInstance.getCluster().getLocalMember().getUuid();
    this.distributedServerManager = distributedServerManager;
    this.messageService = messageService;
  }

  @Override
  public OClusterDBConfig getDatabaseConfig(String dbName) {
    return dbConfigs.get(dbName);
  }

  @Override
  public void setDatabaseConfig(String dbName, OClusterDBConfig dbConfig) {
    dbConfigs.put(dbName, dbConfig); // todo: return the old one?
  }

  @Override
  public ODistributedServerManager.DB_STATUS getDatabaseStatus(String iNode, String iDatabaseName) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(iDatabaseName)) {
      // CHECK THE SERVER STATUS
      if (getActiveServers().contains(iNode)) {
        return ODistributedServerManager.DB_STATUS.ONLINE;
      } else {
        return ODistributedServerManager.DB_STATUS.NOT_AVAILABLE;
      }
    }

    final ODistributedServerManager.DB_STATUS status =
        (ODistributedServerManager.DB_STATUS)
            hzDistributedMap.getLocalCachedValue(
                OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX + iNode + "." + iDatabaseName);
    return status != null ? status : ODistributedServerManager.DB_STATUS.NOT_AVAILABLE;
  }

  @Override
  public ODistributedServerManager.NODE_STATUS getNodeStatus() {
    return status;
  }

  @Override
  public void setNodeStatus(ODistributedServerManager.NODE_STATUS status) {
    if (status.equals(status))
      // NO CHANGE
      return;

    status = status;

    ODistributedServerLog.info(
        this,
        nodeName,
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "Updated node status to '%s'",
        status);
  }

  @Override
  public String getNodeName(String id) {
    return null;
  }

  @Override
  public String getLocalNodeName() {
    return nodeName;
  }

  @Override
  public Set<String> getActiveServers() {
    return activeNodesByName.keySet();
  }

  @Override
  public ODocument getClusterConfiguration() {
    final ODocument cluster = new ODocument();

    cluster.field("localName", "cluster");
    cluster.field("localId", nodeUuid);

    // INSERT MEMBERS
    final List<ODocument> members = new ArrayList<ODocument>();
    cluster.field("members", members, OType.EMBEDDEDLIST);
    for (Member member : activeNodesByName.values()) {
      members.add(getNodeConfigurationByUuid(member.getUuid(), true));
    }

    return cluster;
  }

  @Override
  public void shutdown() {
    activeNodesByName.clear();
    activeNodesNamesByUuid.clear();
    activeNodesUuidByName.clear();
  }

  @Override
  public ODocument getNodeConfigurationByUuid(final String uuid, final boolean useCache) {
    if (hzDistributedMap == null)
      // NOT YET STARTED
      return null;

    final ODocument doc;
    if (useCache) {
      doc =
          (ODocument)
              hzDistributedMap.getLocalCachedValue(OHazelcastPlugin.CONFIG_NODE_PREFIX + uuid);
    } else {
      doc = (ODocument) hzDistributedMap.get(OHazelcastPlugin.CONFIG_NODE_PREFIX + uuid);
    }

    if (doc == null)
      ODistributedServerLog.debug(
          this,
          nodeName,
          null,
          ODistributedServerLog.DIRECTION.OUT,
          "Cannot find node with id '%s'",
          uuid);

    return doc;
  }

  public ODocument getNodeConfigurationByName(String nodeName) {
    Member member = getClusterMemberByName(nodeName);
    return getNodeConfigurationByUuid(member.getUuid(), false);
  }

  @Override
  public Set<String> getActiveNodes(String databaseName) {
    Set<String> nodes = new HashSet<>();
    for (Map.Entry<String, Member> entry : activeNodesByName.entrySet()) {
      if (isNodeAvailable(entry.getKey(), databaseName)) {
        nodes.add(entry.getKey());
      }
    }
    return nodes;
  }

  @Override
  public boolean isNodeAvailable(String nodeName, String databaseName) {
    final ODistributedServerManager.DB_STATUS s = getDatabaseStatus(nodeName, databaseName);
    return s != ODistributedServerManager.DB_STATUS.OFFLINE
        && s != ODistributedServerManager.DB_STATUS.NOT_AVAILABLE;
  }

  @Override
  public ODistributedStrategy getDistributedStrategy() {
    return distributedStrategy;
  }

  @Override
  public void setDistributedStrategy(ODistributedStrategy distributedStrategy) {
    this.distributedStrategy = distributedStrategy;
  }

  public void addActiveNode(String name, Member member) {
    activeNodesByName.put(name, member);
    if (member.getUuid() != null) {
      activeNodesNamesByUuid.put(member.getUuid(), name);
      activeNodesUuidByName.put(name, member.getUuid());
    }
  }

  public void removeActiveNode(String name, Member member) {
    activeNodesByName.remove(name);
    activeNodesNamesByUuid.remove(member.getUuid());
    activeNodesUuidByName.remove(name);
  }

  public OHazelcastDistributedMap getHzDistributedMap() {
    return hzDistributedMap;
  }

  public String getNodeNameByUuid(String uuid) {
    return activeNodesNamesByUuid.get(uuid);
  }

  public String getNodeUuidByName(String name) {
    return activeNodesUuidByName.get(name);
  }

  public Map<String, Member> getActiveNodes() {
    return activeNodesByName;
  }

  @Override
  public boolean isNodeAvailable(final String nodeName) {
    if (nodeName == null) return false;
    Member member = activeNodesByName.get(nodeName);
    return member != null && hz.getCluster().getMembers().contains(member);
  }

  private Member getClusterMemberByName(final String rNodeName) {
    Member member = activeNodesByName.get(rNodeName);
    if (member == null) {
      // SYNC PROBLEMS? TRY TO RETRIEVE THE SERVER INFORMATION FROM THE CLUSTER MAP
      for (Iterator<Map.Entry<String, Object>> it = hzDistributedMap.localEntrySet().iterator();
          it.hasNext(); ) {
        final Map.Entry<String, Object> entry = it.next();
        if (entry.getKey().startsWith(OHazelcastPlugin.CONFIG_NODE_PREFIX)) {
          final ODocument nodeCfg = (ODocument) entry.getValue();
          if (rNodeName.equals(nodeCfg.field("name"))) {
            // FOUND: USE THIS
            final String uuid =
                entry.getKey().substring(OHazelcastPlugin.CONFIG_NODE_PREFIX.length());

            for (Member m : hz.getCluster().getMembers()) {
              if (m.getUuid().equals(uuid)) {
                member = m;
                registerNode(member, rNodeName);
                break;
              }
            }
          }
        }
      }

      if (member == null) throw new ODistributedException("Cannot find node '" + rNodeName + "'");
    }
    return member;
  }

  private void tryRegisterNode(
      final Member member,
      final String joinedNodeName,
      List<ODistributedLifecycleListener> listeners) {
    if (activeNodesByName.containsKey(joinedNodeName))
      // ALREADY REGISTERED: SKIP IT
      return;

    if (joinedNodeName.startsWith("ext:"))
      // NODE HAS NOT IS YET
      return;

    if (activeNodesByName.putIfAbsent(joinedNodeName, member) == null) {
      // NOTIFY NODE IS GOING TO BE ADDED. IS EVERYBODY OK?
      for (ODistributedLifecycleListener l : listeners) {
        if (!l.onNodeJoining(joinedNodeName)) {
          // DENY JOIN
          ODistributedServerLog.info(
              this,
              nodeName,
              getNodeName(member, true),
              ODistributedServerLog.DIRECTION.IN,
              "Denied node to join the cluster id=%s name=%s",
              member,
              getNodeName(member, true));

          activeNodesByName.remove(joinedNodeName);
          return;
        }
      }

      activeNodesNamesByUuid.put(member.getUuid(), joinedNodeName);
      activeNodesUuidByName.put(joinedNodeName, member.getUuid());
      ORemoteServerController network = null;
      try {
        network = distributedServerManager.getRemoteServer(joinedNodeName);
      } catch (IOException e) {
        ODistributedServerLog.error(
            this,
            nodeName,
            joinedNodeName,
            ODistributedServerLog.DIRECTION.OUT,
            "Error on connecting to node %s",
            joinedNodeName);
      }

      ODistributedServerLog.info(
          this,
          nodeName,
          getNodeName(member, true),
          ODistributedServerLog.DIRECTION.IN,
          "Added node configuration id=%s name=%s, now %d nodes are configured",
          member,
          getNodeName(member, true),
          getActiveServers());

      // NOTIFY NODE WAS ADDED SUCCESSFULLY
      for (ODistributedLifecycleListener l : listeners) l.onNodeJoined(joinedNodeName);

      // FORCE THE ALIGNMENT FOR ALL THE ONLINE DATABASES AFTER THE JOIN ONLY IF AUTO-DEPLOY IS SET
      for (String db : getManagedDatabases()) {
        if (getDatabaseConfiguration(db).isAutoDeploy()
            && getDatabaseStatus(joinedNodeName, db) == ODistributedServerManager.DB_STATUS.ONLINE) {
          setDatabaseStatus(joinedNodeName, db, ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);
        }
      }
      dumpServersStatus();
    }
  }

  public String getNodeName(final Member iMember, final boolean useCache) {
    if (iMember == null || iMember.getUuid() == null) return "?";

    if (nodeUuid.equals(iMember.getUuid()))
      // LOCAL NODE (NOT YET NAMED)
      return nodeName;

    final String name = getNodeNameByUuid(iMember.getUuid());
    if (name != null) return name;

    final ODocument cfg = getNodeConfigurationByUuid(iMember.getUuid(), useCache);
    if (cfg != null) return cfg.field("name");

    return "ext:" + iMember.getUuid();
  }

  public void HzNodeStateChanged(final LifecycleEvent event) {
    final LifecycleEvent.LifecycleState state = event.getState();
    if (state == LifecycleEvent.LifecycleState.MERGING)
      setNodeStatus(ODistributedServerManager.NODE_STATUS.MERGING);
    else if (state == LifecycleEvent.LifecycleState.MERGED) {
      ODistributedServerLog.info(
          this,
          nodeName,
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Server merged the existent cluster, lock=%s, merging databases...",
          getLockManagerServer());

      hzDistributedMap.clearLocalCache();

      // UPDATE THE UUID
      final String oldUuid = nodeUuid;
      nodeUuid = hz.getCluster().getLocalMember().getUuid();

      ODistributedServerLog.info(
          this,
          nodeName,
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Replacing old UUID %s with the new %s",
          oldUuid,
          nodeUuid);

      activeNodesNamesByUuid.remove(oldUuid);
      hzDistributedMap.remove(OHazelcastPlugin.CONFIG_NODE_PREFIX + oldUuid);

      activeNodesByName.put(nodeName, hz.getCluster().getLocalMember());
      activeNodesNamesByUuid.put(nodeUuid, nodeName);
      activeNodesUuidByName.put(nodeName, nodeUuid);

      publishLocalNodeConfiguration();
      setNodeStatus(ODistributedServerManager.NODE_STATUS.ONLINE);

      // TEMPORARY PATCH TO FIX HAZELCAST'S BEHAVIOUR THAT ENQUEUES THE MERGING ITEM EVENT WITH THIS
      // AND ACTIVE NODES MAP COULD BE STILL NOT FILLED
      Thread t =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  try {
                    // WAIT (MAX 10 SECS) THE LOCK MANAGER IS ONLINE
                    ODistributedServerLog.info(
                        this,
                        getLocalNodeName(),
                        null,
                        ODistributedServerLog.DIRECTION.NONE,
                        "Merging networks, waiting for the lock %s to be reachable...",
                        getLockManagerServer());

                    for (int retry = 0;
                        !getActiveServers().contains(getLockManagerServer()) && retry < 10;
                        ++retry) {
                      try {
                        Thread.sleep(1000);
                      } catch (InterruptedException e) {
                        // IGNORE IT
                      }
                    }

                    final String cs = getLockManagerServer();

                    ODistributedServerLog.info(
                        this,
                        getLocalNodeName(),
                        null,
                        ODistributedServerLog.DIRECTION.NONE,
                        "Merging networks, lock=%s (active=%s)...",
                        cs,
                        getActiveServers().contains(getLockManagerServer()));

                    for (final String databaseName : getManagedDatabases()) {
                      distributedServerManager.executeInDistributedDatabaseLock(
                          databaseName,
                          20000,
                          null,
                          new OCallable<Object, OModifiableDistributedConfiguration>() {
                            @Override
                            public Object call(final OModifiableDistributedConfiguration cfg) {
                              ODistributedServerLog.debug(
                                  this,
                                  getLocalNodeName(),
                                  null,
                                  ODistributedServerLog.DIRECTION.NONE,
                                  "Replacing local database '%s' configuration with the most recent from the joined cluster...",
                                  databaseName);

                              cfg.override(
                                  (ODocument)
                                      hzDistributedMap.get(
                                          OHazelcastPlugin.CONFIG_DATABASE_PREFIX + databaseName));
                              return null;
                            }
                          });
                    }
                  } finally {
                    ODistributedServerLog.warn(
                        this,
                        getLocalNodeName(),
                        null,
                        ODistributedServerLog.DIRECTION.NONE,
                        "Network merged, lock=%s...",
                        getLockManagerServer());
                    setNodeStatus(ODistributedServerManager.NODE_STATUS.ONLINE);
                  }
                }
              });
      t.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
      t.start();
    }
  }

  void publishLocalNodeConfiguration() {
    try {
      final ODocument cfg = getLocalNodeConfiguration();
      ORecordInternal.setRecordSerializer(cfg, ODatabaseDocumentAbstract.getDefaultSerializer());
      hzDistributedMap.put(OHazelcastPlugin.CONFIG_NODE_PREFIX + nodeUuid, cfg);
    } catch (Exception e) {
      ODistributedServerLog.error(
          this,
          nodeName,
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Error on publishing local server configuration",
          e);
    }
  }

  private ODocument getLocalNodeConfiguration() {
    final ODocument nodeCfg = new ODocument();
    nodeCfg.setTrackingChanges(false);

    nodeCfg.field("id", distributedServerManager.getLocalNodeId());
    nodeCfg.field("uuid", nodeUuid);
    nodeCfg.field("name", nodeName);
    nodeCfg.field("version", OConstants.getRawVersion());
    nodeCfg.field("publicAddress", distributedServerManager.getPublicAddress());
    nodeCfg.field("startedOn", distributedServerManager.getStartedOn());
    nodeCfg.field("status", getNodeStatus());
    nodeCfg.field("connections", distributedServerManager.getClientConnectionManager().getTotal());

    final List<Map<String, Object>> listeners = new ArrayList<Map<String, Object>>();
    nodeCfg.field("listeners", listeners, OType.EMBEDDEDLIST);

    for (OServerNetworkListener listener : distributedServerManager.getNetworkListeners()) {
      final Map<String, Object> listenerCfg = new HashMap<String, Object>();
      listeners.add(listenerCfg);

      listenerCfg.put("protocol", listener.getProtocolType().getSimpleName());
      listenerCfg.put("listen", listener.getListeningAddress(true));
    }

    // STORE THE TEMP USER/PASSWD USED FOR REPLICATION
    final OSecurityUser user = distributedServerManager.getReplicatorUser();
    if (user != null) {
      nodeCfg.field("user_replicator", user.getPassword());
    }

    nodeCfg.field("databases", getManagedDatabases());

    final long maxMem = Runtime.getRuntime().maxMemory();
    final long totMem = Runtime.getRuntime().totalMemory();
    final long freeMem = Runtime.getRuntime().freeMemory();
    final long usedMem = totMem - freeMem;

    nodeCfg.field("usedMemory", usedMem);
    nodeCfg.field("freeMemory", freeMem);
    nodeCfg.field("maxMemory", maxMem);

    nodeCfg.field("latencies", messageService.getLatencies(), OType.EMBEDDED);
    nodeCfg.field("messages", messageService.getMessageStats(), OType.EMBEDDED);

    for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners();
        it.hasNext(); ) {
      final ODatabaseLifecycleListener listener = it.next();
      if (listener != null) listener.onLocalNodeConfigurationRequest(nodeCfg);
    }

    return nodeCfg;
  }

  private String getLockManagerServer() {
    return "";
  }

  private Set<String> getManagedDatabases() {
    return messageService != null ? messageService.getDatabases() : Collections.EMPTY_SET;
  }
}
