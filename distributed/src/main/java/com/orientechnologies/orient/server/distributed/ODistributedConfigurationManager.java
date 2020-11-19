package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentAbstract;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.cluster.OClusterDBConfig;
import com.orientechnologies.orient.server.distributed.cluster.OClusterDBNode;
import com.orientechnologies.orient.server.distributed.cluster.OClusterMetadataManager;
import com.orientechnologies.orient.server.distributed.impl.ODistributedOutput;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

// HC-based distributed config manager
public class ODistributedConfigurationManager implements OClusterDBConfig {

  private final OClusterMetadataManager clusterMetadataManager;
  private volatile ODistributedConfiguration distributedConfiguration;
  private final String databaseName;
  private final String localNodeName;
  private final String databaseDir;
  private final Map<String, Object> configMap;
  private final File defaultConfigFile;

  public ODistributedConfigurationManager(
      OClusterMetadataManager clusterMetadataManager,
      Map<String, Object> configMap,
      String localNodeName,
      String databaseDir,
      String dbName,
      File defaultConfigFile) {
    this.configMap = configMap;
    this.databaseName = dbName;
    this.localNodeName = localNodeName;
    this.databaseDir = databaseDir;
    this.defaultConfigFile = defaultConfigFile;
    this.clusterMetadataManager = clusterMetadataManager;
  }

  @Override
  public Set<OClusterDBNode> getNodes() {
    return null;
  }

  @Override
  public long getVersion() {
    return distributedConfiguration.getVersion();
  }

  @Override
  public int getReplicationFactor() {
    return 0;
  }

  @Override
  public ODistributedConfiguration getDistributedConfiguration() {
    if (distributedConfiguration == null) {
      if (configMap == null) return null;

      ODocument doc =
          (ODocument) configMap.get(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + databaseName);
      if (doc != null) {
        // DISTRIBUTED CFG AVAILABLE: COPY IT TO THE LOCAL DIRECTORY
        ODistributedServerLog.info(
            this,
            localNodeName,
            null,
            ODistributedServerLog.DIRECTION.NONE,
            "Downloaded configuration for database '%s' from the cluster",
            databaseName);
        setDistributedConfiguration(new OModifiableDistributedConfiguration(doc));
      } else {
        doc = loadDatabaseConfiguration(getDistributedConfigFile());
        if (doc == null) {
          // LOOK FOR THE STD FILE
          doc = loadDatabaseConfiguration(defaultConfigFile);
          if (doc == null)
            throw new OConfigurationException(
                "Cannot load default distributed for database '"
                    + databaseName
                    + "' config file: "
                    + defaultConfigFile);

          // SAVE THE GENERIC FILE AS DATABASE FILE
          setDistributedConfiguration(new OModifiableDistributedConfiguration(doc));
        } else
          // JUST LOAD THE FILE IN MEMORY
          distributedConfiguration = new ODistributedConfiguration(doc);

        // LOADED FILE, PUBLISH IT IN THE CLUSTER
        updateDistributedConfiguration(new OModifiableDistributedConfiguration(doc));
      }
    }
    return distributedConfiguration;
  }

  @Override
  public void setDistributedConfiguration(
      final OModifiableDistributedConfiguration distributedConfiguration) {
    if (this.distributedConfiguration == null
        || distributedConfiguration.getVersion() > this.distributedConfiguration.getVersion()) {
      this.distributedConfiguration =
          new ODistributedConfiguration(distributedConfiguration.getDocument().copy());

      // PRINT THE NEW CONFIGURATION
      final String cfgOutput =
          ODistributedOutput.formatClusterTable(clusterMetadataManager, databaseName);

      ODistributedServerLog.info(
          this,
          localNodeName,
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Setting new distributed configuration for database: %s (version=%d)%s\n",
          databaseName,
          distributedConfiguration.getVersion(),
          cfgOutput);

      saveDistributedConfiguration();
    }
  }

  private ODocument loadDatabaseConfiguration(final File file) {
    if (!file.exists() || file.length() == 0) return null;

    ODistributedServerLog.info(
        this,
        localNodeName,
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "Loaded configuration for database '%s' from disk: %s",
        databaseName,
        file);

    FileInputStream f = null;
    try {
      f = new FileInputStream(file);
      final byte[] buffer = new byte[(int) file.length()];
      f.read(buffer);

      final ODocument doc = new ODocument().fromJSON(new String(buffer), "noMap");
      doc.field("version", 1);
      return doc;

    } catch (Exception e) {
      ODistributedServerLog.error(
          this,
          localNodeName,
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Error on loading distributed configuration file in: %s",
          e,
          file.getAbsolutePath());
    } finally {
      if (f != null)
        try {
          f.close();
        } catch (IOException e) {
        }
    }
    return null;
  }

  @Override
  public void saveDistributedConfiguration() {
    // SAVE THE CONFIGURATION TO DISK
    FileOutputStream f = null;
    try {
      File file = getDistributedConfigFile();

      ODistributedServerLog.debug(
          this,
          localNodeName,
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Saving distributed configuration file for database '%s' to: %s",
          databaseName,
          file);

      if (!file.exists()) {
        file.getParentFile().mkdirs();
        file.createNewFile();
      }

      f = new FileOutputStream(file);
      f.write(distributedConfiguration.getDocument().toJSON().getBytes());
      f.flush();
    } catch (Exception e) {
      ODistributedServerLog.error(
          this,
          localNodeName,
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Error on saving distributed configuration file",
          e);

    } finally {
      if (f != null)
        try {
          f.close();
        } catch (IOException e) {
        }
    }
  }

  @Override
  public boolean updateDistributedConfiguration(OModifiableDistributedConfiguration cfg) {
    clusterMetadataManager.getDistributedStrategy().validateConfiguration(cfg);
    final ODistributedConfiguration dCfg = getDistributedConfiguration();

    ODocument oldCfg = dCfg != null ? dCfg.getDocument() : null;
    Integer oldVersion = oldCfg != null ? (Integer) oldCfg.field("version") : null;
    if (oldVersion == null) oldVersion = 0;

    int currVersion = cfg.getVersion();

    boolean updated = currVersion > oldVersion;

    if (oldCfg != null && !updated) {
      // NO CHANGE, SKIP IT
      OLogManager.instance()
          .debug(
              this,
              "Skip saving of distributed configuration file for database '%s' because is unchanged (version %d)",
              databaseName,
              currVersion);
      updated = false;
    }

    setDistributedConfiguration(cfg);

    ODistributedServerLog.info(
        this,
        localNodeName,
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "Broadcasting new distributed configuration for database: %s (version=%d)\n",
        databaseName,
        currVersion);

    if (!updated && !configMap.containsKey(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + databaseName))
      // FIRST TIME, FORCE PUBLISHING
      updated = true;

    final ODocument document = cfg.getDocument();

    if (updated) {

      // WRITE TO THE MAP TO BE READ BY NEW SERVERS ON JOIN
      ORecordInternal.setRecordSerializer(
          document, ODatabaseDocumentAbstract.getDefaultSerializer());
      configMap.put(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + databaseName, document);
    }

    return updated;
  }

  protected File getDistributedConfigFile() {
    return new File(
        databaseDir + databaseName + "/" + ODistributedServerManager.FILE_DISTRIBUTED_DB_CONFIG);
  }
}
