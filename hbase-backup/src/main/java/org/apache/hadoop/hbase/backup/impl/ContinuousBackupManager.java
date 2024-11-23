package org.apache.hadoop.hbase.backup.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@InterfaceAudience.Private
public class ContinuousBackupManager {
  private static final Logger LOG = LoggerFactory.getLogger(ContinuousBackupManager.class);
  public static final String CONF_BACKUP_ROOT_DIR = "hbase.backup.root.dir";
  public static final String CONF_BACKUP_MAX_WAL_SIZE = "hbase.backup.max.wal.size";
  public static final long DEFAULT_MAX_WAL_SIZE = 128 * 1024 * 1024;
  public static final String WALS_DIR = "WALs";
  public static final String BULKLOAD_FILES_DIR = "bulk-load-files";

  private final Configuration conf;
  private final FileSystem backupFs;
  private final Path backupRootDir;
  private Path walsDir;
  private Path bulkLoadFilesDir;
  private final ContinuousBackupStagingManager continuousBackupStagingManager;

  public ContinuousBackupManager(Configuration conf) throws BackupConfigurationException {
    this.conf = conf;

    LOG.info("Initializing ContinuesBackupManager with provided configuration.");

    String backupRootDirStr = conf.get(CONF_BACKUP_ROOT_DIR);
    if (backupRootDirStr == null || backupRootDirStr.isEmpty()) {
      LOG.error("Backup root directory not specified in configuration: {}", CONF_BACKUP_ROOT_DIR);
      throw new BackupConfigurationException("Backup root directory not specified");
    }

    backupRootDir = new Path(backupRootDirStr);

    try {
      LOG.info("Initializing backup file system for directory: {}", backupRootDirStr);
      backupFs = FileSystem.get(backupRootDir.toUri(), conf);
      initBackupFileSystem();
      LOG.info("Backup file system initialized successfully for directory: {}", backupRootDirStr);
    } catch (IOException e) {
      LOG.error("Failed to initialize backup file system for directory: {}", backupRootDirStr, e);
      throw new BackupConfigurationException("Failed to initialize backup file system for directory: "
        + backupRootDirStr, e);
    }

    try {
      LOG.info("Initializing ContinuesBackupStagingManager with provided configuration.");
      continuousBackupStagingManager = new ContinuousBackupStagingManager(conf, this);
      LOG.info("ContinuesBackupStagingManager initialized successfully.");
    } catch (IOException e) {
      LOG.error("Failed to initialize ContinuesBackupStagingManager: ", e);
      throw new BackupConfigurationException("Failed to initialize ContinuesBackupStagingManager", e);
    }
  }

  private void initBackupFileSystem() throws IOException {
    initWalsDir();
    initBulkLoadFilesDir();
  }

  private void initWalsDir() throws IOException {
    walsDir = new Path(backupRootDir, WALS_DIR);
    if (backupFs.exists(walsDir)) {
      LOG.info("WALs directory already present: {}", walsDir);
      return;
    }
    backupFs.mkdirs(walsDir);
    LOG.info("WALs directory directory created: {}", walsDir);
  }

  private void initBulkLoadFilesDir() throws IOException {
    bulkLoadFilesDir = new Path(backupRootDir, BULKLOAD_FILES_DIR);
    if (backupFs.exists(bulkLoadFilesDir)) {
      LOG.info("Bulk Load Files directory already present: {}", bulkLoadFilesDir);
      return;
    }
    backupFs.mkdirs(bulkLoadFilesDir);
    LOG.info("Bulk Load Files directory created: {}", bulkLoadFilesDir);
  }

  public void accept(Map<TableName, List<WAL.Entry>> tableToEntriesMap) throws IOException {
    for (Map.Entry<TableName, List<WAL.Entry>> entry : tableToEntriesMap.entrySet()) {
      TableName tableName = entry.getKey();
      List<WAL.Entry> walEntries = entry.getValue();
      List<Path> bulkLoadFiles = getBulkLoadFiles(tableName, walEntries);
      for (Path p : bulkLoadFiles) {
        LOG.info("Bulkload File: {}", p.toString());
      }
      continuousBackupStagingManager.stageEntries(tableName, walEntries, bulkLoadFiles);
    }
  }

  private List<Path> getBulkLoadFiles(TableName tableName, List<WAL.Entry> walEntries)
    throws IOException {
    List<Path> bulkLoadFilePaths = new ArrayList<>();
    String namespace = tableName.getNamespaceAsString();
    String table = tableName.getQualifierAsString();
    LOG.debug("Handling bulk load entries for table: {}:{}", namespace, table);

    for (WAL.Entry entry : walEntries) {
      WALEdit edit = entry.getEdit();
      List<Cell> cells = edit.getCells();
      LOG.debug("Number of cells in the edit: {}", cells.size());

      for (Cell cell : cells) {
        if (CellUtil.matchingQualifier(cell, WALEdit.BULK_LOAD)) {
          processBulkLoadDescriptor(entry, bulkLoadFilePaths, cell, namespace, table);
        } else {
          LOG.debug("Skipping non-bulk-load cell.");
        }
      }
    }
    return bulkLoadFilePaths;
  }

  private void processBulkLoadDescriptor(WAL.Entry entry, List<Path> bulkLoadFilePaths, Cell cell,
    String namespace, String table) throws IOException {
    WALProtos.BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cell);

    if (bld == null) {
      LOG.warn("BulkLoadDescriptor is null for entry: {}", entry);
      return;
    }

    if (!bld.getReplicate()) {
      LOG.debug("Replication is disabled for bulk load entry: {}", entry);
      return;
    }

    String regionName = bld.getEncodedRegionName() != null ? bld.getEncodedRegionName().toStringUtf8() : null;
    if (regionName == null) {
      LOG.warn("Encoded region name is null for bulk load descriptor.");
      return;
    }

    List<WALProtos.StoreDescriptor> storesList = bld.getStoresList();
    if (storesList == null) {
      LOG.warn("Store descriptor list is null for region: {}", regionName);
      return;
    }

    for (WALProtos.StoreDescriptor storeDescriptor : storesList) {
      processStoreDescriptor(storeDescriptor, namespace, table, regionName, bulkLoadFilePaths);
    }
  }

  private void processStoreDescriptor(WALProtos.StoreDescriptor storeDescriptor, String namespace, String table, String regionName, List<Path> bulkLoadFilePaths) {
    String columnFamilyName = storeDescriptor.getFamilyName().toStringUtf8();
    LOG.debug("Processing column family: {}", columnFamilyName);

    List<String> storeFileList = storeDescriptor.getStoreFileList();
    if (storeFileList == null) {
      LOG.warn("Store file list is null for column family: {}", columnFamilyName);
      return;
    }

    for (String storeFile : storeFileList) {
      Path hFilePath = getHFilePath(namespace, table, regionName, columnFamilyName, storeFile);
      LOG.debug("Adding HFile path to bulk load file paths: {}", hFilePath);
      bulkLoadFilePaths.add(hFilePath);
    }
  }

  private Path getHFilePath(String namespace, String tableName, String regionName,
    String columnFamilyName, String storeFileName) {
    return new Path(namespace, new Path(tableName, new Path(regionName, new Path(columnFamilyName, storeFileName))));
  }

  public void backup(FileSystem sourceFs, Path walFile, List<Path> bulkLoadFiles) throws IOException {
    Path sourcePath = continuousBackupStagingManager.getWalFileStagingPath(walFile);
    Path backupWalPath = new Path(walsDir, walFile);
    FileUtil.copy(sourceFs, sourcePath, backupFs, backupWalPath, true, conf);
    uploadBulkloadFiles(sourceFs, bulkLoadFiles);
  }

  private void uploadBulkloadFiles(FileSystem sourceFs, List<Path> bulkLoadFiles)
    throws IOException {
    for (Path file : bulkLoadFiles) {
      Path sourePath = continuousBackupStagingManager.getBulkloadFileStagingPath(file);
      Path destPath = new Path(bulkLoadFilesDir, file);
      FileUtil.copy(sourceFs, sourePath, backupFs, destPath, false, conf);
    }
  }

  public void stop() {
    continuousBackupStagingManager.stop();
  }
}
