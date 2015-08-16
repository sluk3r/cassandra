package org.apache.cassandra.db.commitlog;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.UUID;

import junit.framework.Assert;

import com.google.common.base.Predicate;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.KeyspaceParams;

//wxc pro 2015-8-16:17:12:08 这样的一些假想， 以后验证： CommitLog应该是CRUD时， 先写到记录文件， 再在方便时， 或异步或批量地处理。 这样的假设有些问题： 一些数据一致性的工作不大可能在这些记录文件里做校验。加到当前测试类的Replay核心上： 是不是Replay对应着系统如果崩溃了， 如果通过commitlog来进一步地降低损失？
public class CommitLogUpgradeTest
{
    static final String DATA_DIR = "test/data/legacy-commitlog/";//wxc 2015-8-16:16:40:47 貌似是存测试数据的地方，是不是应该有clean操作？
    static final String PROPERTIES_FILE = "hash.txt";//wxc 2015-8-16:16:41:57 找到此文件了， 不止一个。DATA_DIR下的以版本号（貌似）命名的目录中都有这么个文件. (应该是每一个版本下都有一批相关文件， 这些文件应该是测试前手工生成的， 只是有测试时用下)打开这个文件看了下。 文件内容上看有三个字段cfid、cells和hash。
    static final String CFID_PROPERTY = "cfid"; //wxc 2015-8-16:16:46:19 下面的这三个是文件名， 而是上面hash.txt文件的key值。
    static final String CELLS_PROPERTY = "cells";
    static final String HASH_PROPERTY = "hash";

    static final String TABLE = "Standard1";
    static final String KEYSPACE = "Keyspace1";
    static final String CELLNAME = "name";

    @Test
    public void test20() throws Exception
    {
        testRestore(DATA_DIR + "2.0");//wxc pro 2015-8-16:16:49:02 每一版本的都要测下， 貌似差异是在数据上了。
    }

    @Test
    public void test21() throws Exception
    {
        testRestore(DATA_DIR + "2.1");
    }

    @Test
    public void test22() throws Exception
    {
        testRestore(DATA_DIR + "2.2");
    }

    @Test
    public void test22_LZ4() throws Exception
    {
        testRestore(DATA_DIR + "2.2-lz4");
    }

    @Test
    public void test22_Snappy() throws Exception
    {
        testRestore(DATA_DIR + "2.2-snappy");
    }



    //wxc 2015-8-16:16:49:55 把这个前戏给忘了。 这个很重要。做应用层面上的初始化工作。
    @BeforeClass
    static public void initialize() throws FileNotFoundException, IOException, InterruptedException
    {
        CFMetaData metadata = CFMetaData.Builder.createDense(KEYSPACE, TABLE, false, false)
                                                .addPartitionKey("key", AsciiType.instance) //wxc 2015-8-16:16:50:40 这样的Type定义以前在追Hiberate时，也看到过类似的命名。 每一次努力都是数据， 数据是可以被理解的。
                                                .addClusteringColumn("col", AsciiType.instance)
                                                .addRegularColumn("val", BytesType.instance)
                                                .build()
                                                .compression(SchemaLoader.getCompressionParameters());
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    metadata);
        //wxc 2015-8-16:16:55:56 不写sql， 直接内部创建KEYSPACE。走后门。
    }

    //wxc 2015-8-16:16:40:00 当前类的核心方法。
    public void testRestore(String location) throws IOException, InterruptedException
    {
        Properties prop = new Properties();
        prop.load(new FileInputStream(new File(location + File.separatorChar + PROPERTIES_FILE)));
        int hash = Integer.parseInt(prop.getProperty(HASH_PROPERTY));
        int cells = Integer.parseInt(prop.getProperty(CELLS_PROPERTY));

        String cfidString = prop.getProperty(CFID_PROPERTY);//wxc 2015-8-16:16:57:26  CF是什么的缩写？
        if (cfidString != null)//wxc 2015-8-16:17:00:27 如果是空代表什么逻辑？
        {
            UUID cfid = UUID.fromString(cfidString);//wxc pro 2015-8-16:16:59:16 为什么要生成UUID， 怎么还要指定一个Seed?
            if (Schema.instance.getCF(cfid) == null)
            {
                CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE, TABLE);
                Schema.instance.unload(cfm);//wxc 2015-8-16:17:01:02 unload是什么个操作？
                Schema.instance.load(cfm.copy(cfid));//wxc pro 2015-8-16:17:02:06 copy时指定cfid意图是？再加一个load操作？
            }
        }

        Hasher hasher = new Hasher();
        CommitLogTestReplayer replayer = new CommitLogTestReplayer(hasher);//wxc 2015-8-16:17:03:34 这个Hasher不仅仅当前类里用到， Replayer里也用到。
        File[] files = new File(location).listFiles(new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String name)
            {
                return name.endsWith(".log");
            }
        });
        replayer.recover(files);//wxc pro 2015-8-16:17:07:16 这个是核心逻辑。 往下再怎么理解？

        Assert.assertEquals(cells, hasher.cells);//wxc pro 2015-8-16:17:05:41 cells和hash的值原始是怎么计算出来的？ 貌似hasher里又计算了遍。
        Assert.assertEquals(hash, hasher.hash);
    }

    public static int hash(int hash, ByteBuffer bytes)
    {
        int shift = 0;
        for (int i = 0; i < bytes.limit(); i++)
        {
            hash += (bytes.get(i) & 0xFF) << shift;
            shift = (shift + 8) & 0x1F;
        }
        return hash;
    }

    //wxc 2015-8-16:16:36:47 先看这个类： 名字是Hasher， 跟Hash有什么关系？ 在当前文件中怎么体现的？ Predicate是怎么个逻辑？ 这是个存疑的地方， 先放一下。
    class Hasher implements Predicate<Mutation>
    {
        int hash = 0;
        int cells = 0;

        @Override
        public boolean apply(Mutation mutation)
        {
            for (PartitionUpdate update : mutation.getPartitionUpdates())
            {
                for (Row row : update)
                    if (row.clustering().size() > 0 &&
                        AsciiType.instance.compose(row.clustering().get(0)).startsWith(CELLNAME))
                    {
                        for (Cell cell : row.cells())
                        {
                            hash = hash(hash, cell.value());
                            ++cells;
                        }
                    }
            }
            return true;
        }
    }
}
