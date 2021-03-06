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
package org.apache.cassandra.utils;

import com.google.common.base.Throwables;

public abstract class WrappedRunnable implements Runnable
{
    public final void run()
    {
        try
        {
            runMayThrow();
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);//wxc pro 2015-8-8:9:00:54 这样的异常背后是什么逻辑？ 自己什么情况下用这个异常？
        }
    }

    abstract protected void runMayThrow() throws Exception;
}
