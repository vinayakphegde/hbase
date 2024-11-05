package org.apache.hadoop.hbase.backup.util;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ContinuousBackup {
  public static final String CONFIG_CONTINUOUS_BACKUP_REPLICATION_ENDPOINT =
    "hbase.continuous.backup.replication.endpoint.impl";
  public static final String DEFAULT_CONTINUOUS_BACKUP_REPLICATION_ENDPOINT =
    "org.apache.hadoop.hbase.backup.impl.ContinuousBackupReplicationEndpoint";
}
