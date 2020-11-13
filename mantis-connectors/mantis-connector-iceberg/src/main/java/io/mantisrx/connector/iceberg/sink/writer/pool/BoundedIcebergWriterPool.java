/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connector.iceberg.sink.writer.pool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.mantisrx.connector.iceberg.sink.writer.IcebergWriter;
import io.mantisrx.connector.iceberg.sink.writer.config.WriterConfig;
import io.mantisrx.connector.iceberg.sink.writer.factory.IcebergWriterFactory;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;

/**
 *
 */
public class BoundedIcebergWriterPool implements IcebergWriterPool {

    private final WriterConfig config;
    private final IcebergWriterFactory factory;
    private final Map<StructLike, IcebergWriter> pool;

    public BoundedIcebergWriterPool(WriterConfig config, IcebergWriterFactory factory) {
        this.config = config;
        this.factory = factory;
        this.pool = new HashMap<>();
    }

    @Override
    public void addWriter(StructLike partition) {
        pool.put(partition, factory.newIcebergWriter());
    }

    @Override
    public void openWriter(StructLike partition) throws IOException {
        IcebergWriter writer = pool.get(partition);
        if (writer == null) {
            throw new IOException("writer does not exist in writer pool");
        }
        writer.open();
    }

    @Override
    public void write(StructLike partition, Record record) {
        IcebergWriter writer = pool.get(partition);
        if (writer == null) {
            throw new RuntimeException("writer does not exist in writer pool");
        }
        writer.write(record);
    }

    @Override
    public DataFile close(StructLike partition) throws IOException, UncheckedIOException {
        IcebergWriter writer = pool.get(partition);
        if (writer == null) {
            throw new RuntimeException("writer does not exist in writer pool");
        }
        return writer.close();
    }

    /**
     * Attempts to close all writers and produce {@link DataFile}s. If a writer is already closed, then it will
     * produce a {@code null} which will be excluded from the resulting list.
     */
    @Override
    public List<DataFile> closeAll() throws IOException, UncheckedIOException {
        List<DataFile> dataFiles = new ArrayList<>();
        for (IcebergWriter writer : pool.values()) {
            DataFile dataFile = writer.close();
            if (dataFile != null) {
                dataFiles.add(dataFile);
            }
        }

        return dataFiles;
    }

    @Override
    public List<StructLike> getFlushableWriters() {
        return pool.entrySet().stream()
                .filter(entry -> entry.getValue().length() >= config.getWriterFlushFrequencyBytes())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    @Override
    public boolean isClosed(StructLike partition) {
        IcebergWriter writer = pool.get(partition);
        if (writer == null) {
            throw new RuntimeException("writer does not exist in writer pool");
        }
        return writer.isClosed();
    }

    @Override
    public boolean hasWriter(StructLike partition) {
        return pool.containsKey(partition);
    }

    @Override
    public boolean isWriterFlushable(StructLike partition) throws UncheckedIOException {
        IcebergWriter writer = pool.get(partition);
        if (writer == null) {
            throw new RuntimeException("writer does not exist in writer pool");
        }
        return writer.length() >= config.getWriterFlushFrequencyBytes();
    }
}
