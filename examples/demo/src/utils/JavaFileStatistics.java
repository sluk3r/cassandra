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

package utils;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.junit.Test;

/**
 * Created by wangxichun on 2015/9/9.
 */
public class JavaFileStatistics
{
    @Test
    public void javaStat() throws IOException
    {
        String path= "F:\\openSrc\\github\\cassandra";
        Path start = Paths.get(path);

        int[] javaContainer = new int[]{0};
        int[] javaNotTestContainer = new int[]{0};

        Files.walkFileTree(start, new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs)
            throws IOException
            {
                String fileName = filePath.toFile().getName();

                if (fileName.endsWith(".java") && !fileName.toString().contains("Test"))
                {
                    javaNotTestContainer[0]++;
                }

                if (fileName.endsWith(".java"))
                {
                    javaContainer[0]++;
                }

                return FileVisitResult.CONTINUE;
            }
        });

        //wxc 2015-9-9:22:29:23 一共1381个不包含Test的Java文件。
        System.out.println("java files cnt(test excepted): " + javaNotTestContainer[0]);
        //wxc 2015-9-9:22:38:22 一共包含着1680个Java文件。
        System.out.println("java files cnt(test contained): " + javaContainer[0]);

    }
}
