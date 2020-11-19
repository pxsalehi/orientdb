package com.orientechnologies.orient.server.distributed.cluster;

import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import java.util.Set;

public interface OClusterDBConfig {
  // Nodes of the cluster that have this database
  Set<OClusterDBNode> getNodes();

  // Current version of the configuration
  long getVersion();

  // Number of nodes that have a leader role for the DB
  int getReplicationFactor();

  // TODO: For now allow get/set of the config as a whole. Maybe later,
  //  hide it behind more fine-grained getters/setters, like above.
  ODistributedConfiguration getDistributedConfiguration();

  void setDistributedConfiguration(
      final OModifiableDistributedConfiguration distributedConfiguration);

  void saveDistributedConfiguration();

  boolean updateDistributedConfiguration(final OModifiableDistributedConfiguration cfg);
}
