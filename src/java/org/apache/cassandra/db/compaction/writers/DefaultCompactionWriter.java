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
package org.apache.cassandra.db.compaction.writers;


import java.io.File;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

/**
 * The default compaction writer - creates one output file in L0
 */
public class DefaultCompactionWriter extends CompactionAwareWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(DefaultCompactionWriter.class);

    @SuppressWarnings("resource")
    public DefaultCompactionWriter(ColumnFamilyStore cfs, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, boolean offline)
    {
        super(cfs, txn, nonExpiredSSTables, offline);
        logger.debug("Expected bloom filter size : {}", estimatedTotalKeys);
        long expectedWriteSize = cfs.getExpectedCompactedFileSize(nonExpiredSSTables, txn.opType());
        File sstableDirectory = cfs.directories.getLocationForDisk(getWriteDirectory(expectedWriteSize));
        @SuppressWarnings("resource")
        SSTableWriter writer = SSTableWriter.create(Descriptor.fromFilename(cfs.getSSTablePath(sstableDirectory)),
                                                    estimatedTotalKeys,
                                                    minRepairedAt,
                                                    cfs.metadata,
                                                    new MetadataCollector(txn.originals(), cfs.metadata.comparator, 0),
                                                    SerializationHeader.make(cfs.metadata, nonExpiredSSTables),
                                                    txn);
        sstableWriter.switchWriter(writer);
    }

    @Override
    public boolean append(UnfilteredRowIterator partition)
    {
        return sstableWriter.append(partition) != null;
    }

    @Override
    public long estimatedKeys()
    {
        return estimatedTotalKeys;
    }
}
