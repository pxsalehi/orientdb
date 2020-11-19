package com.orientechnologies.orient.server.distributed.cluster;

import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;

public interface OClusterDBNode {
  ODistributedConfiguration.ROLES getRole();

  String getName();
}
