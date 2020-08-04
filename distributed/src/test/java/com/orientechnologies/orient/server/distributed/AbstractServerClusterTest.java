/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.distributed;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import com.orientechnologies.orient.setup.LocalTestSetup;
import com.orientechnologies.orient.setup.ServerRun;
import com.orientechnologies.orient.setup.TestConfig;
import org.junit.Assert;

/**
 * Test class that creates and executes distributed operations against a cluster of servers created
 * in the same JVM.
 */
public abstract class AbstractServerClusterTest {
  protected int delayServerStartup = 0;
  protected int delayServerAlign = 0;
  protected boolean startupNodesInSequence = true;
  protected boolean terminateAtShutdown = true;

  private LocalTestSetup localSetup;
  private TestConfig testConfig;

  protected AbstractServerClusterTest() {
    OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL.setValue(true);
  }

  public void init(final TestConfig testConfig) {
    this.testConfig = testConfig;
    ODatabaseDocumentTx.closeAll();
    Orient.setRegisterDatabaseByPath(true);
    localSetup = new LocalTestSetup(testConfig);
  }

  public void execute() throws Exception {
    System.out.println(
        "Starting test against " + testConfig.getServerIds().size() + " server nodes...");

    try {
      startServers();
      banner("Executing test...");
      try {
        executeTest();
      } finally {
        onAfterExecution();
      }
    } catch (Exception e) {
      System.out.println("ERROR: ");
      e.printStackTrace();
      OLogManager.instance().flush();
      throw e;
    } finally {
      banner("Test finished");

      OLogManager.instance().flush();
      banner("Shutting down " + testConfig.getServerIds().size() + " nodes...");
      for (ServerRun server : localSetup.getServers()) {
        log("Shutting down node " + server.getServerId() + "...");
        if (terminateAtShutdown) server.terminateServer();
        else server.shutdownServer();
      }

      ODatabaseDocumentTx.closeAll();
      onTestEnded();

      banner("Terminate HZ...");
      for (HazelcastInstance in : Hazelcast.getAllHazelcastInstances()) {
        if (terminateAtShutdown)
          // TERMINATE (HARD SHUTDOWN)
          in.getLifecycleService().terminate();
        else
          // SOFT SHUTDOWN
          in.shutdown();
      }

      banner("Clean server directories...");
      deleteServers();
    }
  }

  protected void startServers() throws Exception {
    Hazelcast.shutdownAll();
    int numberOfServers = testConfig.getServerIds().size();
    if (startupNodesInSequence) {
      startServersInSequence();
    } else {
      startServersInParallel();
    }

    if (delayServerAlign > 0)
      try {
        System.out.println(
            "Server started, waiting for synchronization ("
                + (delayServerAlign * numberOfServers / 1000)
                + "secs)...");
        Thread.sleep(delayServerAlign * numberOfServers);
      } catch (InterruptedException e) {
      }

    for (ServerRun server : localSetup.getServers()) {
      final ODistributedServerManager mgr = server.getServerInstance().getDistributedManager();
      Assert.assertNotNull(mgr);
      final ODocument cfg = mgr.getClusterConfiguration();
      Assert.assertNotNull(cfg);
    }
  }

  private void startServersInSequence() throws Exception {
    for (final ServerRun server : localSetup.getServers()) {
      banner("STARTING SERVER -> " + server.getServerId() + "...");

      server.startServer(getDistributedServerConfiguration(server));

      if (delayServerStartup > 0)
        try {
          Thread.sleep(delayServerStartup * testConfig.getServerIds().size());
        } catch (InterruptedException e) {
        }

      onServerStarted(server);
    }
  }

  private void startServersInParallel() {
    for (final ServerRun server : localSetup.getServers()) {
      final Thread thread =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  banner("STARTING SERVER -> " + server.getServerId() + "...");
                  try {
                    onServerStarting(server);
                    server.startServer(getDistributedServerConfiguration(server));
                    onServerStarted(server);
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                }
              });
      thread.start();
    }
  }

  protected void banner(final String iMessage) {
    OLogManager.instance()
        .error(
            this,
            "**********************************************************************************************************",
            null);
    OLogManager.instance().error(this, iMessage, null);
    OLogManager.instance()
        .error(
            this,
            "**********************************************************************************************************",
            null);
  }

  protected void log(final String iMessage) {
    OLogManager.instance().info(this, iMessage);
  }

  protected void onServerStarting(ServerRun server) {}

  protected void onServerStarted(ServerRun server) {}

  protected void onTestEnded() {}

  protected void onBeforeExecution() throws Exception {}

  protected void onAfterExecution() throws Exception {}

  protected void createDatabase(final String serverId) {
    ServerRun server = localSetup.getServer(serverId);
    if (server != null) {
      server
          .getServerInstance()
          .createDatabase(getDatabaseName(), ODatabaseType.PLOCAL, OrientDBConfig.defaultConfig());
    }
  }

  protected boolean databaseExists(final String serverId) {
    ServerRun server = localSetup.getServer(serverId);
    if (server != null) {
      return server.getServerInstance().existsDatabase(getDatabaseName());
    }
    return false;
  }

  protected ODatabaseDocument getDatabase() {
    return getDatabase(testConfig.getServerIds().get(0));
  }

  protected ODatabaseDocumentInternal getDatabase(final String serverId) {
    ServerRun server = localSetup.getServer(serverId);
    if (server != null) return getDatabase(server);
    return null;
  }

  protected ODatabaseDocumentInternal getDatabase(final ServerRun serverRun) {
    if (serverRun != null) {
      return serverRun.getServerInstance().openDatabase(getDatabaseName(), "admin", "admin");
    }

    return null;
  }

  protected OVertex getVertex(final ODatabaseDocument db, final ORID orid) {
    return getVertex(db.load(orid));
  }

  protected OVertex getVertex(final ODocument doc) {
    if (doc != null) return doc.asVertex().get();

    return null;
  }

  protected abstract String getDatabaseName();

  /**
   * Event called right after the database has been created and right before to be replicated to the
   * X servers
   *
   * @param db Current database
   */
  protected void onAfterDatabaseCreation(final ODatabaseDocument db) {}

  protected abstract void executeTest() throws Exception;

  protected void prepare(final boolean iCopyDatabaseToNodes) throws Exception {
    prepare(iCopyDatabaseToNodes, true);
  }

  /** Create the database on first node only */
  protected void prepare(final boolean iCopyDatabaseToNodes, final boolean iCreateDatabase)
      throws Exception {
    // CREATE THE DATABASE
    final Iterator<ServerRun> it = localSetup.getServers().iterator();
    final ServerRun master = it.next();

    if (iCreateDatabase) {
      OrientDB orientDB =
          new OrientDB(
              "embedded:" + master.getServerHome() + "/databases/", OrientDBConfig.defaultConfig());
      orientDB.create(getDatabaseName(), ODatabaseType.PLOCAL);
      final ODatabaseDocument graph = orientDB.open(getDatabaseName(), "admin", "admin");
      try {
        onAfterDatabaseCreation(graph);
      } finally {
        graph.close();
        orientDB.close();
      }
    }

    // COPY DATABASE TO OTHER SERVERS
    while (it.hasNext()) {
      final ServerRun replicaSrv = it.next();

      replicaSrv.deleteNode();

      if (iCopyDatabaseToNodes)
        master.copyDatabase(getDatabaseName(), replicaSrv.getDatabasePath(getDatabaseName()));
    }
  }

  protected void deleteServers() {
    for (ServerRun s : localSetup.getServers()) s.deleteNode();

    Hazelcast.shutdownAll();
  }

  protected String getDistributedServerConfiguration(final ServerRun server) {
    return testConfig.getLocalConfigFile(server.getServerId());
  }

  protected void executeWhen(final Callable<Boolean> condition, final Callable action)
      throws Exception {
    while (true) {
      if (condition.call()) {
        action.call();
        break;
      }

      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        // IGNORE IT
      }
    }
  }

  protected void executeWhen(
      String serverId,
      OCallable<Boolean, ODatabaseDocument> condition,
      OCallable<Boolean, ODatabaseDocument> action)
      throws Exception {
    final ODatabaseDocument db = getDatabase(serverId);
    try {
      executeWhen(db, condition, action);
    } finally {
      if (!db.isClosed()) {
        db.activateOnCurrentThread();
        db.close();
      }
    }
  }

  protected void executeWhen(
      final ODatabaseDocument db,
      OCallable<Boolean, ODatabaseDocument> condition,
      OCallable<Boolean, ODatabaseDocument> action) {
    while (true) {
      db.activateOnCurrentThread();
      if (condition.call(db)) {
        action.call(db);
        break;
      }

      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        // IGNORE IT
      }
    }
  }

  protected void assertDatabaseStatusEquals(
      final String fromServerId,
      final String serverName,
      final String dbName,
      final ODistributedServerManager.DB_STATUS status) {
    Assert.assertEquals(
        status,
        localSetup
            .getServer(fromServerId)
            .getServerInstance()
            .getDistributedManager()
            .getDatabaseStatus(serverName, dbName));
  }

  protected void waitForDatabaseStatus(
      final String serverId,
      final String serverName,
      final String dbName,
      final ODistributedServerManager.DB_STATUS status,
      final long timeout) {
    final long startTime = System.currentTimeMillis();
    while (localSetup
            .getServer(serverId)
            .getServerInstance()
            .getDistributedManager()
            .getDatabaseStatus(serverName, dbName)
        != status) {

      if (timeout > 0 && System.currentTimeMillis() - startTime > timeout) {
        OLogManager.instance()
            .error(this, "TIMEOUT on wait-for condition (timeout=" + timeout + ")", null);
        break;
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // IGNORE IT
      }
    }
  }

  protected void waitForDatabaseIsOffline(
      final String serverName, final String dbName, final long timeout) {
    final long startTime = System.currentTimeMillis();
    String server0Id = testConfig.getServerIds().get(0);
    while (localSetup
        .getServer(server0Id)
        .getServerInstance()
        .getDistributedManager()
        .isNodeOnline(serverName, dbName)) {

      if (timeout > 0 && System.currentTimeMillis() - startTime > timeout) {
        OLogManager.instance()
            .error(this, "TIMEOUT on wait-for condition (timeout=" + timeout + ")", null);
        break;
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // IGNORE IT
      }
    }
  }

  protected void waitForDatabaseIsOnline(
      final String fromServerId, final String serverName, final String dbName, final long timeout) {
    final long startTime = System.currentTimeMillis();
    while (!localSetup
        .getServer(fromServerId)
        .getServerInstance()
        .getDistributedManager()
        .isNodeOnline(serverName, dbName)) {

      if (timeout > 0 && System.currentTimeMillis() - startTime > timeout) {
        OLogManager.instance()
            .error(
                this,
                "TIMEOUT on waitForDatabaseIsOnline condition (timeout=" + timeout + ")",
                null);
        break;
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // IGNORE IT
      }
    }
  }

  protected void waitFor(
      final String serverId,
      final OCallable<Boolean, ODatabaseDocument> condition,
      final long timeout) {
    try {
      ODatabaseDocument db = getDatabase(serverId);
      try {

        final long startTime = System.currentTimeMillis();

        while (true) {
          if (condition.call(db)) {
            break;
          }

          if (timeout > 0 && System.currentTimeMillis() - startTime > timeout) {
            OLogManager.instance()
                .error(this, "TIMEOUT on wait-for condition (timeout=" + timeout + ")", null);
            break;
          }

          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            // IGNORE IT
          }
        }

      } finally {
        if (!db.isClosed()) {
          db.activateOnCurrentThread();
          db.close();
        }
      }
    } catch (Exception e) {
      // INGORE IT
    }
  }

  protected void waitFor(
      final long timeout, final OCallable<Boolean, Void> condition, final String message) {
    final long startTime = System.currentTimeMillis();

    while (true) {
      if (condition.call(null)) {
        // SUCCEED
        break;
      }

      if (timeout > 0 && System.currentTimeMillis() - startTime > timeout)
        throw new OTimeoutException("Timeout waiting for test condition: " + message);

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // IGNORE IT
      }
    }
  }

  protected String getDatabaseURL(ServerRun server) {
    return null;
  }

  protected List<ServerRun> createServerList(final String... serverIds) {
    final List<ServerRun> result = new ArrayList<ServerRun>(serverIds.length);
    for (String s : serverIds) result.add(localSetup.getServer(s));
    return result;
  }

  protected void checkSameClusters() {
    List<String> clusters = null;
    for (ServerRun s : localSetup.getServers()) {
      ODatabaseDocument d = getDatabase(s);
      try {
        d.reload();

        final List<String> dbClusters = new ArrayList<String>(d.getClusterNames());
        Collections.sort(dbClusters);

        if (clusters == null) {
          clusters = new ArrayList<String>(dbClusters);
        } else {
          Assert.assertEquals(
              "Clusters are not the same number. server0="
                  + clusters
                  + " server"
                  + s.getServerId()
                  + "="
                  + dbClusters,
              clusters.size(),
              dbClusters.size());
        }
      } finally {
        d.activateOnCurrentThread();
        d.close();
      }
    }
  }
}
