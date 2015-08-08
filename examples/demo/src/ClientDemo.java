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

import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**
 * Created by wangxichun on 2015/8/8.
 * 用ClientDemo连接Debug方式启动的Server， 发请求， 看Request的处理路径。 同时也体会下在自己的Application中怎么用Cassandra的API。 另外注意的还有jdbc版的API
 */
public class ClientDemo
{
    final static String KEYSPACE = "demo_keyspace";
    final static String TABLE_NAME = "demo_name";
    final static String INDEX_NAME = "demo_index";

    Session sessionOper = null;

    @Before
    public void setUp () {
        Cluster cluster = Cluster.builder()
                                 .addContactPoint("127.0.0.1")
                                 .build();

        assertNotNull(cluster);

        Metadata metadata = cluster.getMetadata();
        System.out.println("connected to server: " + metadata.getClusterName());

        Session session = cluster.connect();

        session.execute(String.format("DROP KEYSPACE IF  EXISTS %s ", KEYSPACE));

        String cqlCreateKeySpace = String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE);
        session.execute(cqlCreateKeySpace);

        session = cluster.connect(KEYSPACE);
        session.execute(String.format("CREATE table %s (a int, b int, primary key(a));", TABLE_NAME));
        session.execute(String.format("CREATE INDEX ON %s(%s)", TABLE_NAME, "b"));

        sessionOper = session;
    }

    @After
    public void cleanUp() {
        sessionOper.execute(String.format("DROP TABLE %s ", TABLE_NAME));
    }


    @Test
    public void demoCRUD() {
        int aValue = 1;
        int bValue = 2;
        sessionOper.execute(
                      QueryBuilder.insertInto(TABLE_NAME)
                                  .values(new String[]{"a","b"}, new Object[]{aValue,bValue}));

        ResultSet result = sessionOper.execute(QueryBuilder.select("a","b")
                                                       .from(TABLE_NAME)
                                                           .where(QueryBuilder.eq("a", aValue))
                                                           .and(QueryBuilder.eq("b", bValue)));
        Iterator<Row> iterator = result.iterator();
        while(iterator.hasNext())
        {
            Row row = iterator.next();
            assertEquals(aValue, row.getInt("a"));
            assertEquals(bValue, row.getInt("b"));
        }
    }
}
