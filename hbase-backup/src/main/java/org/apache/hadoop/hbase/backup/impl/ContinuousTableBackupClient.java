package org.apache.hadoop.hbase.backup.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupCopyJob;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupRestoreFactory;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.master.LogRollMasterProcedureManager;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfigBuilder;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.JOB_NAME_CONF_KEY;
import static org.apache.hadoop.hbase.backup.util.ContinuousBackup.CONFIG_CONTINUOUS_BACKUP_REPLICATION_ENDPOINT;
import static org.apache.hadoop.hbase.backup.util.ContinuousBackup.DEFAULT_CONTINUOUS_BACKUP_REPLICATION_ENDPOINT;

@InterfaceAudience.Private
public class ContinuousTableBackupClient extends TableBackupClient {
  private static final Logger LOG = LoggerFactory.getLogger(ContinuousTableBackupClient.class);
  public ContinuousTableBackupClient(final Connection conn, final String backupId, BackupRequest request)
    throws IOException {
    super(conn, backupId, request);
  }

  @Override
  public void execute() throws IOException {
    try (Admin admin = conn.getAdmin()) {
      // Begin BACKUP
      beginBackup(backupManager, backupInfo);

      // Start WAL Replication to back up Location
      backupInfo.setPhase(BackupInfo.BackupPhase.SETUP_WAL_REPLICATION);
      startContinuousWALBackup(admin);


//      String savedStartCode;
//      boolean firstBackup;
//      // do snapshot for full table backup
//
//      savedStartCode = backupManager.readBackupStartCode();
//      firstBackup = savedStartCode == null || Long.parseLong(savedStartCode) == 0L;
//      if (firstBackup) {
//        // This is our first backup. Let's put some marker to system table so that we can hold the
//        // logs while we do the backup.
//        backupManager.writeBackupStartCode(0L);
//      }
      // We roll log here before we do the snapshot. It is possible there is duplicate data
      // in the log that is already in the snapshot. But if we do it after the snapshot, we
      // could have data loss.
      // A better approach is to do the roll log on each RS in the same global procedure as
      // the snapshot.
//      LOG.info("Execute roll log procedure for full backup ...");
//
//      Map<String, String> props = new HashMap<>();
//      props.put("backupRoot", backupInfo.getBackupRootDir());
//      admin.execProcedure(LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_SIGNATURE,
//        LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_NAME, props);

//      newTimestamps = backupManager.readRegionServerLastLogRollResult();

      // SNAPSHOT_TABLES:
      backupInfo.setPhase(BackupInfo.BackupPhase.SNAPSHOT);
      for (TableName tableName : tableList) {
        String snapshotName = "snapshot_" + EnvironmentEdgeManager.currentTime()
          + "_" + tableName.getNamespaceAsString() + "_" + tableName.getQualifierAsString();

        snapshotTable(admin, tableName, snapshotName);
        backupInfo.setSnapshotName(tableName, snapshotName);
      }

      // SNAPSHOT_COPY:
      // do snapshot copy
      LOG.debug("snapshot copy for " + backupId);
      snapshotCopy(backupInfo);
//      // Updates incremental backup table set
//      backupManager.addIncrementalBackupTableSet(backupInfo.getTables());

      // BACKUP_COMPLETE:
      // set overall backup status: complete. Here we make sure to complete the backup.
      // After this checkpoint, even if entering cancel process, will let the backup finished
      backupInfo.setState(BackupInfo.BackupState.COMPLETE);
//      // The table list in backupInfo is good for both full backup and incremental backup.
//      // For incremental backup, it contains the incremental backup table set.
//      backupManager.writeRegionServerLogTimestamp(backupInfo.getTables(), newTimestamps);

//      Map<TableName, Map<String, Long>> newTableSetTimestampMap =
//        backupManager.readLogTimestampMap();

//      backupInfo.setTableSetTimestampMap(newTableSetTimestampMap);
//      Long newStartCode =
//        BackupUtils.getMinValue(BackupUtils.getRSLogTimestampMins(newTableSetTimestampMap));
//      backupManager.writeBackupStartCode(newStartCode);

      // backup complete
      completeBackup(conn, backupInfo, BackupType.CONTINUOUS, conf);
    } catch (Exception e) {
      failBackup(conn, backupInfo, backupManager, e, "Unexpected BackupException : ",
        BackupType.CONTINUOUS, conf);
      throw new IOException(e);
    }
  }

  private void startContinuousWALBackup(Admin admin) throws IOException {
    // Construct replication peer
    Map<String, String> additionalArgs = backupInfo.getAdditionalArgs();

    String continuousBackupReplicationEndpoint = additionalArgs.getOrDefault(
      CONFIG_CONTINUOUS_BACKUP_REPLICATION_ENDPOINT, DEFAULT_CONTINUOUS_BACKUP_REPLICATION_ENDPOINT);

    String rootDir = backupInfo.getBackupRootDir() + Path.SEPARATOR + backupInfo.getBackupId();
    additionalArgs.put("hbase.backup.root.dir", rootDir);

    Map<TableName, List<String>> tableMap = tableList.stream()
      .collect(Collectors.toMap(tableName -> tableName, tableName -> new ArrayList<>()));

    ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setReplicationEndpointImpl(continuousBackupReplicationEndpoint)
      .setReplicateAllUserTables(false)
      .setTableCFsMap(tableMap)
      .putAllConfiguration(additionalArgs)
      .build();

    // Add Replication Peer
    try {
      admin.addReplicationPeer(backupId, peerConfig, true);
      LOG.info("Successfully added replication peer with backup ID: {}", backupId);
    } catch (IOException e) {
      LOG.error("Failed to add replication peer with backup ID: {}. Error: {}", backupId,
        e.getMessage(), e);
      throw e;
    }
  }

  /**
   * Do snapshot copy.
   * @param backupInfo backup info
   * @throws Exception exception
   */
  protected void snapshotCopy(BackupInfo backupInfo) throws Exception {
    LOG.info("Snapshot copy is starting.");

    // set overall backup phase: snapshot_copy
    backupInfo.setPhase(BackupInfo.BackupPhase.SNAPSHOTCOPY);

    // call ExportSnapshot to copy files based on hbase snapshot for backup
    // ExportSnapshot only support single snapshot export, need loop for multiple tables case
    BackupCopyJob copyService = BackupRestoreFactory.getBackupCopyJob(conf);

    // number of snapshots matches number of tables
    float numOfSnapshots = backupInfo.getSnapshotNames().size();

    LOG.debug("There are " + (int) numOfSnapshots + " snapshots to be copied.");

    for (TableName table : backupInfo.getTables()) {
      // Currently we simply set the sub copy tasks by counting the table snapshot number, we can
      // calculate the real files' size for the percentage in the future.
      // backupCopier.setSubTaskPercntgInWholeTask(1f / numOfSnapshots);
      int res;
      ArrayList<String> argsList = new ArrayList<>();
      argsList.add("-snapshot");
      argsList.add(backupInfo.getSnapshotName(table));
      argsList.add("-copy-to");
      argsList.add(backupInfo.getTableBackupDir(table) + Path.SEPARATOR + getSnapshotTs(backupInfo.getSnapshotName(table)));
      if (backupInfo.getBandwidth() > -1) {
        argsList.add("-bandwidth");
        argsList.add(String.valueOf(backupInfo.getBandwidth()));
      }
      if (backupInfo.getWorkers() > -1) {
        argsList.add("-mappers");
        argsList.add(String.valueOf(backupInfo.getWorkers()));
      }
      if (backupInfo.getNoChecksumVerify()) {
        argsList.add("-no-checksum-verify");
      }

      String[] args = argsList.toArray(new String[0]);

      String jobname = "Full-Backup_" + backupInfo.getBackupId() + "_" + table.getNameAsString();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Setting snapshot copy job name to : " + jobname);
      }
      conf.set(JOB_NAME_CONF_KEY, jobname);

      LOG.debug("Copy snapshot " + args[1] + " to " + args[3]);
      res = copyService.copy(backupInfo, backupManager, conf, BackupType.FULL, args);

      // if one snapshot export failed, do not continue for remained snapshots
      if (res != 0) {
        LOG.error("Exporting Snapshot " + args[1] + " failed with return code: " + res + ".");

        throw new IOException("Failed of exporting snapshot " + args[1] + " to " + args[3]
          + " with reason code " + res);
      }

      conf.unset(JOB_NAME_CONF_KEY);
      LOG.info("Snapshot copy " + args[1] + " finished.");
    }
  }

  private String getSnapshotTs(String snapshotName) {
    return snapshotName.split("_")[1];
  }
}

