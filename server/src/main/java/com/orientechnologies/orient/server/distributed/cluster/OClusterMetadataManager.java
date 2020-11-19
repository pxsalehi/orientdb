package com.orientechnologies.orient.server.distributed.cluster;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedStrategy;
import java.util.Set;

public interface OClusterMetadataManager {

  OClusterDBConfig getDatabaseConfig(String dbName);

  void setDatabaseConfig(String dbName, OClusterDBConfig dbConfig);

  ODistributedServerManager.DB_STATUS getDatabaseStatus(String node, String databaseName);

  ODistributedServerManager.NODE_STATUS getNodeStatus();

  void setNodeStatus(ODistributedServerManager.NODE_STATUS status);

  String getNodeName(String id);

  String getLocalNodeName();

  Set<String> getActiveServers();

  boolean isNodeAvailable(String nodeName);

  boolean isNodeAvailable(String nodeName, String databaseName);

  Set<String> getActiveNodes(final String databaseName);

  ODocument getClusterConfiguration();

  ODocument getNodeConfigurationByUuid(final String uuid, final boolean useCache);

  ODistributedStrategy getDistributedStrategy();

  void setDistributedStrategy(ODistributedStrategy distributedStrategy);

  void shutdown();
}
