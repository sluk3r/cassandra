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

//wxc pro 2015-8-16:17:12:08 ������һЩ���룬 �Ժ���֤�� CommitLogӦ����CRUDʱ�� ��д����¼�ļ��� ���ڷ���ʱ�� ���첽�������ش��� �����ļ�����Щ���⣺ һЩ����һ���ԵĹ��������������Щ��¼�ļ�����У�顣�ӵ���ǰ�������Replay�����ϣ� �ǲ���Replay��Ӧ��ϵͳ��������ˣ� ���ͨ��commitlog����һ���ؽ�����ʧ��
public class CommitLogUpgradeTest
{
    static final String DATA_DIR = "test/data/legacy-commitlog/";//wxc 2015-8-16:16:40:47 ò���Ǵ�������ݵĵط����ǲ���Ӧ����clean������
    static final String PROPERTIES_FILE = "hash.txt";//wxc 2015-8-16:16:41:57 �ҵ����ļ��ˣ� ��ֹһ����DATA_DIR�µ��԰汾�ţ�ò�ƣ�������Ŀ¼�ж�����ô���ļ�. (Ӧ����ÿһ���汾�¶���һ������ļ��� ��Щ�ļ�Ӧ���ǲ���ǰ�ֹ����ɵģ� ֻ���в���ʱ����)������ļ������¡� �ļ������Ͽ��������ֶ�cfid��cells��hash��
    static final String CFID_PROPERTY = "cfid"; //wxc 2015-8-16:16:46:19 ��������������ļ����� ��������hash.txt�ļ���keyֵ��
    static final String CELLS_PROPERTY = "cells";
    static final String HASH_PROPERTY = "hash";

    static final String TABLE = "Standard1";
    static final String KEYSPACE = "Keyspace1";
    static final String CELLNAME = "name";

    @Test
    public void test20() throws Exception
    {
        testRestore(DATA_DIR + "2.0");//wxc pro 2015-8-16:16:49:02 ÿһ�汾�Ķ�Ҫ���£� ò�Ʋ��������������ˡ�
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



    //wxc 2015-8-16:16:49:55 �����ǰϷ�����ˡ� �������Ҫ����Ӧ�ò����ϵĳ�ʼ��������
    @BeforeClass
    static public void initialize() throws FileNotFoundException, IOException, InterruptedException
    {
        CFMetaData metadata = CFMetaData.Builder.createDense(KEYSPACE, TABLE, false, false)
                                                .addPartitionKey("key", AsciiType.instance) //wxc 2015-8-16:16:50:40 ������Type������ǰ��׷Hiberateʱ��Ҳ���������Ƶ������� ÿһ��Ŭ���������ݣ� �����ǿ��Ա����ġ�
                                                .addClusteringColumn("col", AsciiType.instance)
                                                .addRegularColumn("val", BytesType.instance)
                                                .build()
                                                .compression(SchemaLoader.getCompressionParameters());
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    metadata);
        //wxc 2015-8-16:16:55:56 ��дsql�� ֱ���ڲ�����KEYSPACE���ߺ��š�
    }

    //wxc 2015-8-16:16:40:00 ��ǰ��ĺ��ķ�����
    public void testRestore(String location) throws IOException, InterruptedException
    {
        Properties prop = new Properties();
        prop.load(new FileInputStream(new File(location + File.separatorChar + PROPERTIES_FILE)));
        int hash = Integer.parseInt(prop.getProperty(HASH_PROPERTY));
        int cells = Integer.parseInt(prop.getProperty(CELLS_PROPERTY));

        String cfidString = prop.getProperty(CFID_PROPERTY);//wxc 2015-8-16:16:57:26  CF��ʲô����д��
        if (cfidString != null)//wxc 2015-8-16:17:00:27 ����ǿմ���ʲô�߼���
        {
            UUID cfid = UUID.fromString(cfidString);//wxc pro 2015-8-16:16:59:16 ΪʲôҪ����UUID�� ��ô��Ҫָ��һ��Seed?
            if (Schema.instance.getCF(cfid) == null)
            {
                CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE, TABLE);
                Schema.instance.unload(cfm);//wxc 2015-8-16:17:01:02 unload��ʲô��������
                Schema.instance.load(cfm.copy(cfid));//wxc pro 2015-8-16:17:02:06 copyʱָ��cfid��ͼ�ǣ��ټ�һ��load������
            }
        }

        Hasher hasher = new Hasher();
        CommitLogTestReplayer replayer = new CommitLogTestReplayer(hasher);//wxc 2015-8-16:17:03:34 ���Hasher��������ǰ�����õ��� Replayer��Ҳ�õ���
        File[] files = new File(location).listFiles(new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String name)
            {
                return name.endsWith(".log");
            }
        });
        replayer.recover(files);//wxc pro 2015-8-16:17:07:16 ����Ǻ����߼��� ��������ô��⣿

        Assert.assertEquals(cells, hasher.cells);//wxc pro 2015-8-16:17:05:41 cells��hash��ֵԭʼ����ô��������ģ� ò��hasher���ּ����˱顣
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

    //wxc 2015-8-16:16:36:47 �ȿ�����ࣺ ������Hasher�� ��Hash��ʲô��ϵ�� �ڵ�ǰ�ļ�����ô���ֵģ� Predicate����ô���߼��� ���Ǹ����ɵĵط��� �ȷ�һ�¡�
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
