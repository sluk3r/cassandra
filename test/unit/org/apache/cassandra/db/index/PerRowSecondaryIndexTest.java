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
package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.*;

public class PerRowSecondaryIndexTest
{

    // test that when index(key) is called on a PRSI index,
    // the data to be indexed can be read using the supplied
    // key. TestIndex.index(key) simply reads the data to be
    // indexed & stashes it in a static variable for inspection
    // in the test.

    private static final String KEYSPACE1 = "PerRowSecondaryIndexTest";
    private static final String CF_INDEXED = "Indexed1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {//wxc 2015-8-19:12:50:33 这个是不是放到父类里好一些？
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    //wxc 2015-8-31:21:00:59 perRowIndexedCFMD 方法里把自定义的Index类设置进去， 再反射地创建出来。 这种方法不错。
                                    SchemaLoader.perRowIndexedCFMD(KEYSPACE1, CF_INDEXED));
    }

    @Before
    public void clearTestStub()
    {
        PerRowSecondaryIndexTest.TestIndex.reset();
    }

    @Test
    public void testIndexInsertAndUpdate()
    {
        int nowInSec = FBUtilities.nowInSeconds();//wxc pro 2015-8-19:12:53:32 相当于时间戳？

        // create a row then test that the configured index instance was able to read the row
        CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE1, CF_INDEXED);
        ColumnDefinition cdef = cfm.getColumnDefinition(new ColumnIdentifier("indexed", true));

        RowUpdateBuilder builder = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), "k1");//wxc 2015-8-31:21:30:31 貌似cfm是存储里最核心的东西。
        builder.add("indexed", ByteBufferUtil.bytes("foo"));//wxc 2015-8-31:21:36:47 前面的ColumnDefinition已经定义好了。
        builder.build().apply();//wxc pro 2015-8-19:12:58:56 apply中是不是有事件产生？


        //wxc 2015-8-31:20:37:35 这种方式不错： 在必经之路上设置一个取变量的地方。
        UnfilteredRowIterator indexedRow = PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_PARTITION;
        assertNotNull(indexedRow);
        assertEquals(ByteBufferUtil.bytes("foo"), UnfilteredRowIterators.filter(indexedRow, nowInSec).next().getCell(cdef).value());

        // update the row and verify what was indexed
        builder = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros() + 1 , "k1");
        builder.add("indexed", ByteBufferUtil.bytes("bar"));
        builder.build().apply();

        indexedRow = PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_PARTITION;
        assertNotNull(indexedRow);
        assertEquals(ByteBufferUtil.bytes("bar"), UnfilteredRowIterators.filter(indexedRow, nowInSec).next().getCell(cdef).value());
        assertTrue(Arrays.equals("k1".getBytes(), PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_KEY.array()));//wxc pro 2015-9-5:18:21:15  这个测试说明了什么？
    }

    @Test
    public void testColumnDelete()
    {
        // issue a column delete and test that the configured index instance was notified to update
        CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE1, CF_INDEXED);

        new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), "k2")
            .noRowMarker()
            .delete("indexed")
            .build()
            .apply();

        UnfilteredRowIterator indexedRow = PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_PARTITION;
        assertNotNull(indexedRow);

        //We filter tombstones now...
        Assert.assertFalse(UnfilteredRowIterators.filter(indexedRow, FBUtilities.nowInSeconds()).hasNext());
        assertTrue(Arrays.equals("k2".getBytes(), PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_KEY.array()));//wxc pro 2015-9-5:18:22:48 这个Case在delete时说明了什么？
    }

    @Test
    public void testRowDelete()
    {
        // issue a row level delete and test that the configured index instance was notified to update
        CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE1, CF_INDEXED);
        RowUpdateBuilder.deleteRow(cfm, FBUtilities.timestampMicros(), "k3").apply();//wxc 2015-9-5:18:23:40 delete row时， 特意封装了一个方法。 留意下。

        UnfilteredRowIterator indexedRow = PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_PARTITION;
        assertNotNull(indexedRow);
        assertNotNull(indexedRow.partitionLevelDeletion());
        Assert.assertFalse(UnfilteredRowIterators.filter(indexedRow, FBUtilities.nowInSeconds()).hasNext());
        assertTrue(Arrays.equals("k3".getBytes(), PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_KEY.array()));
    }

    @Test
    public void testInvalidSearch()
    {

        CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE1, CF_INDEXED);

        RowUpdateBuilder builder = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), "k1");
        builder.add("indexed", ByteBufferUtil.bytes("foo"));
        builder.build().apply();

        
        // test we can search:
        UntypedResultSet result = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".\"Indexed1\" WHERE indexed = 'foo'", KEYSPACE1));
        assertEquals(1, result.size());

        // test we can't search if the searcher doesn't validate the expression:
        try
        {
            QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".\"Indexed1\" WHERE indexed = 'invalid'", KEYSPACE1));
            fail("Query should have been invalid!");
        }
        catch (Exception e)
        {
            assertTrue(e instanceof InvalidRequestException || (e.getCause() != null && (e.getCause() instanceof InvalidRequestException)));
        }
    }

    //wxc pro 2015-8-31:20:44:07 一个问题： 这个TestIndex是什么时候注入到整体机制里的？
    public static class TestIndex extends PerRowSecondaryIndex
    {
        public static UnfilteredRowIterator LAST_INDEXED_PARTITION;
        public static ByteBuffer LAST_INDEXED_KEY;

        public static void reset()
        {
            LAST_INDEXED_KEY = null;
            LAST_INDEXED_PARTITION = null;
        }

        @Override
        public void index(ByteBuffer rowKey, UnfilteredRowIterator cf)
        {
            LAST_INDEXED_PARTITION = cf;
            LAST_INDEXED_KEY = rowKey;
        }

        public void index(ByteBuffer rowKey, PartitionUpdate atoms)
        {
            LAST_INDEXED_PARTITION = atoms.unfilteredIterator();
            LAST_INDEXED_KEY = rowKey;
        }

        @Override
        public void delete(ByteBuffer key, OpOrder.Group opGroup)
        {
        }

        @Override
        public void init()
        {
        }

        @Override
        public void reload()
        {
        }

        @Override
        public void validateOptions() throws ConfigurationException
        {
        }

        @Override
        public String getIndexName()
        {
            return null;
        }

        @Override
        protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ColumnDefinition> columns)
        {
            return new SecondaryIndexSearcher(baseCfs.indexManager, columns)
            {
                @Override
                public UnfilteredPartitionIterator search(ReadCommand filter, ReadOrderGroup orderGroup)
                {
                    return new SingletonUnfilteredPartitionIterator(LAST_INDEXED_PARTITION, false);
                }

                @Override
                public RowFilter.Expression primaryClause(ReadCommand command)
                {
                    RowFilter.Expression expression = command.rowFilter().iterator().next();

                    if (expression.getIndexValue().equals(ByteBufferUtil.bytes("invalid")))
                        throw new InvalidRequestException("Invalid search!");

                    return expression;
                }

                protected UnfilteredPartitionIterator queryDataFromIndex(AbstractSimplePerColumnSecondaryIndex index, DecoratedKey indexKey, RowIterator indexHits, ReadCommand command, ReadOrderGroup orderGroup)
                {
                    // As we override 'search()' directly for the test, we don't care about this.
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public void forceBlockingFlush()
        {
        }

        @Override
        public ColumnFamilyStore getIndexCfs()
        {
            return baseCfs;
        }

        @Override
        public boolean indexes(ColumnDefinition name)
        {
            return true;
        }

        @Override
        public void validate(DecoratedKey partitionKey) throws InvalidRequestException
        {

        }

        @Override
        public void validate(Clustering clustering) throws InvalidRequestException
        {

        }

        @Override
        public void validate(ByteBuffer cellValue, CellPath path) throws InvalidRequestException
        {

        }

        @Override
        public void removeIndex(ByteBuffer columnName)
        {
        }

        @Override
        public void invalidate()
        {
        }

        @Override
        public void truncateBlocking(long truncatedAt)
        {
        }

        @Override
        public long estimateResultRows() {
            return 0;
        }
    }
}
