package org.apache.hadoop.hbase.backup.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@InterfaceAudience.Private
public class ContinuousBackupStagingManager {
  private static final Logger LOG = LoggerFactory.getLogger(ContinuousBackupStagingManager.class);
  public static final String WALS_BACKUP_STAGING_DIR = "wal-backup-staging";
  public static final String WAL_FILE_PREFIX = "WAL_FILE.";

  private final Configuration conf;
  private final FileSystem rootFs;
  private final Path walsStagingDir;
  private final ConcurrentHashMap<Path, WalWriterContext> walWriterMap = new ConcurrentHashMap<>();
  private final ContinuousBackupManager continuousBackupManager;
  private ScheduledExecutorService flushExecutor;

  public ContinuousBackupStagingManager(Configuration conf, ContinuousBackupManager continuousBackupManager) throws IOException {
    this.conf = conf;
    this.continuousBackupManager = continuousBackupManager;
    this.rootFs = CommonFSUtils.getRootDirFileSystem(conf);
    this.walsStagingDir = new Path(CommonFSUtils.getRootDir(conf), WALS_BACKUP_STAGING_DIR);
    initWalsStagingDir();
    startWalFlushThread();
  }

  private void initWalsStagingDir() throws IOException {
    if (rootFs.exists(walsStagingDir)) {
      LOG.info("WALs Staging Directory {} already exists", walsStagingDir);
      return;
    }
    rootFs.mkdirs(walsStagingDir);
    LOG.info("WALs Staging Directory created: {}", walsStagingDir);
  }

  public void stageEntries(TableName tableName, List<WAL.Entry> walEntries, List<Path> bulkLoadFiles) throws IOException {
    String namespace = tableName.getNamespaceAsString();
    String table = tableName.getQualifierAsString();

    Path walDir = getWalDir(namespace, table);
    WalWriterContext walWriterContext = getWalWriterContext(walDir);
    WALProvider.Writer writer = walWriterContext.getWriter();

    for (WAL.Entry entry : walEntries) {
      writer.append(entry);
    }

    writer.sync(true); // TODO: what should we pass for sync here?
    walWriterContext.addBulkLoadFiles(bulkLoadFiles);
    long maxWalSize = conf.getLong(ContinuousBackupManager.CONF_BACKUP_MAX_WAL_SIZE, ContinuousBackupManager.DEFAULT_MAX_WAL_SIZE);
    if (writer.getLength() >= maxWalSize) {
      doBackup(walDir, walWriterContext);
    }
  }

  public void doBackup(Path walDir, WalWriterContext walWriterContext) throws IOException {
    WALProvider.Writer writer = walWriterContext.getWriter();

    closeWriter(writer);

    continuousBackupManager.backup(rootFs, walWriterContext.getWalPath(), walWriterContext.getBulkLoadFiles());
    removeWalWriterContext(walDir);
  }

  private void closeWriter(WALProvider.Writer writer) {
    if (writer != null) {
      try {
        writer.close();
        LOG.info("WAL writer closed successfully.");
      } catch (IOException e) {
        LOG.error("Error occurred while closing WAL writer: ", e);
      }
    }
  }

  private Path getWalDir(String namespace, String table) {
    return new Path(namespace, table);
  }

  private WalWriterContext getWalWriterContext(Path walDir) throws IOException {
    WalWriterContext context = walWriterMap.get(walDir);
    if (context == null) {
      context = createNewWalWriterContext(walDir);
      walWriterMap.put(walDir, context);
    }
    return context;
  }

  private WalWriterContext createNewWalWriterContext(Path walDir) throws IOException {
    long currentTime = EnvironmentEdgeManager.getDelegate().currentTime();
    Path walDirFullPath = new Path(walsStagingDir, walDir);
    if (!rootFs.exists(walDirFullPath)) {
      rootFs.mkdirs(walDirFullPath);
    }
    String walFileName = WAL_FILE_PREFIX + currentTime;
    Path walFilePath = new Path(walDir, walFileName);
    Path walFileFullPath = new Path(walDirFullPath, walFileName);
    WALProvider.Writer writer = WALFactory.createWALWriter(rootFs, walFileFullPath, conf);
    LOG.info("WAL writer created successfully for {}", walFileFullPath);
    return new WalWriterContext(writer, walFilePath);
  }

  private void removeWalWriterContext(Path walDir) {
    walWriterMap.remove(walDir);
    LOG.info("Removed WAL writer context for {}", walDir);
  }

  public static class WalWriterContext {
    private final WALProvider.Writer writer;
    private final Path walPath;
    private final long initialWalFileSize;
    private final List<Path> bulkLoadFiles = new ArrayList<>();

    public WalWriterContext(WALProvider.Writer writer, Path walPath) {
      this.writer = writer;
      this.walPath = walPath;
      this.initialWalFileSize = writer.getLength();
    }

    public WALProvider.Writer getWriter() {
      return writer;
    }

    public Path getWalPath() {
      return walPath;
    }

    public boolean hasAnyEntry() {
      return writer.getLength() > initialWalFileSize;
    }

    public void addBulkLoadFiles(List<Path> bulkLoadFiles) {
      this.bulkLoadFiles.addAll(bulkLoadFiles);
    }

    public List<Path> getBulkLoadFiles() {
      return bulkLoadFiles;
    }
  }

  public Path getBulkloadFileStagingPath(Path relativePathFromNamespace) throws IOException {
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path baseNamespaceDir = new Path(rootDir, new Path(HConstants.BASE_NAMESPACE_DIR));
    Path hFileArchiveDir = new Path(rootDir, new Path(HConstants.HFILE_ARCHIVE_DIRECTORY, baseNamespaceDir));
    return findExistingPath(baseNamespaceDir, hFileArchiveDir, relativePathFromNamespace);
  }

  private Path findExistingPath(Path baseNamespaceDir, Path hFileArchiveDir, Path filePath) throws IOException {
    Path sourcePath = new Path(baseNamespaceDir, filePath);
    if (rootFs.exists(sourcePath)) {
      return sourcePath;
    }
    sourcePath = new Path(hFileArchiveDir, filePath);
    if (rootFs.exists(sourcePath)) {
      return sourcePath;
    }
    return null;
  }

  public Path getWalFileStagingPath(Path relativeWalPath) {
    return new Path(walsStagingDir, relativeWalPath);
  }

  private void startWalFlushThread() {
    flushExecutor = Executors.newSingleThreadScheduledExecutor();
    flushExecutor.scheduleAtFixedRate(() -> {
      try {
        LOG.info("Periodic WAL flush triggered...");
        flushAndBackup();
      } catch (Exception e) {
        LOG.error("Error during periodic WAL flush: ", e);
      }
    }, 5, 5, TimeUnit.MINUTES); // Initial delay of 5 minutes, then repeat every 5 minutes
  }

  private void flushAndBackup() throws IOException {
    for (Map.Entry<Path, WalWriterContext> entry : walWriterMap.entrySet()) {
      Path walDir = entry.getKey();
      WalWriterContext walWriterContext = entry.getValue();
      if (!walWriterContext.hasAnyEntry()) {
        LOG.info("No WAL data to flush and backup for {}", walDir);
        continue;
      }
      doBackup(walDir, walWriterContext);
    }
  }

  public void stop() {
    stopWalFlushThread();
  }

  private void stopWalFlushThread() {
    flushExecutor.shutdown(); // Stop the flush thread
    try {
      if (!flushExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        flushExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      flushExecutor.shutdownNow();
      Thread.currentThread().interrupt(); // Restore interrupted status
    }
    LOG.info("WAL flush thread stopped.");
  }
}

