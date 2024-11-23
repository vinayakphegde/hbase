package org.apache.hadoop.hbase.backup.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@InterfaceAudience.Private
public class ContinuousBackupReplicationEndpoint extends BaseReplicationEndpoint {
  private static final Logger LOG = LoggerFactory.getLogger(ContinuousBackupReplicationEndpoint.class);
  private ContinuousBackupManager continuousBackupManager;
  private UUID peerUUID; // TODO: not sure whether we need it or not.

  @Override
  public void init(Context context) throws IOException {
    super.init(context);
    LOG.info("Initializing HbaseS3BackupEndpoint");

    Configuration peerConf = this.ctx.getConfiguration();
    Configuration conf = HBaseConfiguration.create(peerConf);

    try {
      continuousBackupManager = new ContinuousBackupManager(conf);
    } catch (BackupConfigurationException e) {
      LOG.error("Failed to initialize ContinuousBackupManager due to configuration issues.", e);
      throw new IOException("Failed to initialize ContinuousBackupManager", e);
    }

    generatePeerUUID();
  }

  @Override
  public UUID getPeerUUID() {
    return peerUUID;
  }

  @Override
  public void start() {
    LOG.info("Starting ContinuousBackupReplicationEndpoint...");
    startAsync();
  }

  @Override
  protected void doStart() {
    LOG.info("ContinuousBackupReplicationEndpoint started.");
    notifyStarted();
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    final List<WAL.Entry> entries = replicateContext.getEntries();
    if (entries.isEmpty()) {
      LOG.warn("No WAL entries to replicate.");
      return true;
    }

    LOG.info("Replicating {} WAL entries.", entries.size());

    Map<TableName, List<WAL.Entry>> tableToEntriesMap = new HashMap<>();

    for (WAL.Entry entry : entries) {
      TableName tableName = entry.getKey().getTableName();
      tableToEntriesMap.computeIfAbsent(tableName, key -> new ArrayList<>()).add(entry);
    }
    LOG.info("Tables to backup: {}", tableToEntriesMap.keySet());

    try {
      LOG.info("Triggering backup for {} tables.", tableToEntriesMap.size());
      continuousBackupManager.accept(tableToEntriesMap);
      LOG.info("Replication completed successfully.");
    } catch (IOException e) {
      LOG.error("Error occurred while writing WAL entries to the backup system for tables: {}",
        tableToEntriesMap.keySet(), e);
      return false;
    }

    return true;
  }

  @Override
  public void stop() {
    LOG.info("Stopping ContinuousBackupReplicationEndpoint...");
    stopAsync();
  }

  @Override
  protected void doStop() {
    continuousBackupManager.stop();
    LOG.info("ContinuousBackupReplicationEndpoint stopped.");
    notifyStopped();
  }

  private void generatePeerUUID() {
    if (peerUUID == null) {
      peerUUID = UUID.randomUUID();
      LOG.info("Generated Peer UUID: {}", peerUUID);
    }
  }
}
