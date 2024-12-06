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
package org.apache.hadoop.hbase.backup.replication;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.regionserver.wal.AbstractProtobufLogWriter;
import org.apache.hadoop.hbase.util.AtomicUtils;
import org.apache.hadoop.hbase.wal.FSHLogProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

@InterfaceAudience.Private
public class ObjectStoreProtobufWalWriter extends AbstractProtobufLogWriter
  implements FSHLogProvider.Writer {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectStoreProtobufWalWriter.class);

  protected FSDataOutputStream output;
  private final AtomicLong syncedLength = new AtomicLong(0);

  @Override
  public void append(WAL.Entry entry) throws IOException {
    entry.getKey().getBuilder(compressor).setFollowingKvCount(entry.getEdit().size()).build()
      .writeDelimitedTo(output);
    for (Cell cell : entry.getEdit().getCells()) {
      // cellEncoder must assume little about the stream, since we write PB and cells in turn.
      cellEncoder.write((ExtendedCell) cell);
    }
    length.set(output.getPos());
  }

  @Override
  public void close() throws IOException {
    if (this.output != null) {
      if (!trailerWritten) {
        writeWALTrailer();
      }
      this.output.close();
      this.output = null;
    }
  }

  @Override
  public void sync(boolean forceSync) throws IOException {
    FSDataOutputStream fsDataOutputstream = this.output;
    if (fsDataOutputstream == null) {
      return; // Presume closed
    }
    fsDataOutputstream.flush();
    AtomicUtils.updateMax(this.syncedLength, fsDataOutputstream.getPos());
  }

  @Override
  public long getSyncedLength() {
    return this.syncedLength.get();
  }

  @Override
  protected void initOutput(FileSystem fs, Path path, boolean overridable, int bufferSize,
    short replication, long blockSize, StreamSlowMonitor monitor, boolean noLocalWrite)
    throws IOException {
    FSDataOutputStreamBuilder<?, ?> builder = fs.createFile(path).overwrite(overridable)
      .bufferSize(bufferSize).replication(replication).blockSize(blockSize);
    this.output = builder.build();
  }

  @Override
  protected void closeOutputIfNecessary() {
    if (this.output != null) {
      try {
        this.output.close();
      } catch (IOException e) {
        LOG.warn("Close output failed", e);
      }
    }
  }

  @Override
  protected long writeMagicAndWALHeader(byte[] magic, WALProtos.WALHeader header)
    throws IOException {
    output.write(magic);
    header.writeDelimitedTo(output);
    return output.getPos();
  }

  @Override
  protected OutputStream getOutputStreamForCellEncoder() {
    return this.output;
  }

  @Override
  protected long writeWALTrailerAndMagic(WALProtos.WALTrailer trailer, byte[] magic)
    throws IOException {
    trailer.writeTo(output);
    output.writeInt(trailer.getSerializedSize());
    output.write(magic);
    return output.getPos();
  }
}
