/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.backup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * POJO class for Update backup request
 */
@InterfaceAudience.Private
public final class UpdateBackupRequest {
  private final String backupId;
  private final String snapshotType;
  private final List<TableName> tableList;
  private final int totalTasks;
  private final long bandwidth;
  private final boolean noChecksumVerify;
  private final String backupSetName;
  private final String yarnPoolName;
  private final Map<String, String> additionalArgs;

  private UpdateBackupRequest(Builder builder) {
    this.backupId = builder.backupId;
    this.snapshotType = builder.snapshotType;
    this.tableList = builder.tableList;
    this.totalTasks = builder.totalTasks;
    this.bandwidth = builder.bandwidth;
    this.noChecksumVerify = builder.noChecksumVerify;
    this.backupSetName = builder.backupSetName;
    this.yarnPoolName = builder.yarnPoolName;
    this.additionalArgs = builder.additionalArgs;
  }

  public String getBackupId() { return backupId; }
  public String getSnapshotType() { return snapshotType; }
  public List<TableName> getTableList() { return tableList; }
  public int getTotalTasks() { return totalTasks; }
  public long getBandwidth() { return bandwidth; }
  public boolean getNoChecksumVerify() { return noChecksumVerify; }
  public String getBackupSetName() { return backupSetName; }
  public String getYarnPoolName() { return yarnPoolName; }
  public Map<String, String> getAdditionalArgs() { return additionalArgs; }

  public static class Builder {

    private String backupId;
    private String snapshotType;
    private List<TableName> tableList;
    private int totalTasks = -1;
    private long bandwidth = -1L;
    private boolean noChecksumVerify = false;
    private String backupSetName;
    private String yarnPoolName;
    private Map<String, String> additionalArgs = new HashMap<>();

    public Builder withBackupId(String backupId) {
      this.backupId = backupId;
      return this;
    }

    public Builder withSnapshotType(String snapshotType) {
      this.snapshotType = snapshotType;
      return this;
    }

    public Builder withTableList(List<TableName> tables) {
      this.tableList = tables;
      return this;
    }

    public Builder withBackupSetName(String setName) {
      this.backupSetName = setName;
      return this;
    }

    public Builder withTotalTasks(int numTasks) {
      this.totalTasks = numTasks;
      return this;
    }

    public Builder withBandwidthPerTasks(int bandwidth) {
      this.bandwidth = bandwidth;
      return this;
    }

    public Builder withNoChecksumVerify(boolean noChecksumVerify) {
      this.noChecksumVerify = noChecksumVerify;
      return this;
    }

    public Builder withYarnPoolName(String name) {
      this.yarnPoolName = name;
      return this;
    }

    public Builder withAdditionalArgs(Map<String, String> additionalArgs) {
      this.additionalArgs = additionalArgs;
      return this;
    }

    public UpdateBackupRequest build() {
      return new UpdateBackupRequest(this);
    }
  }
}
