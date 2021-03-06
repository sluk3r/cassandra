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
package org.apache.cassandra.service;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMIServerSocketFactory;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;

import com.addthis.metrics3.reporter.config.ReporterConfig;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.schema.LegacySchemaMigrator;
import org.apache.cassandra.cql3.functions.ThreadAwareSecurityManager;
import org.apache.cassandra.thrift.ThriftServer;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;

/**
 * The <code>CassandraDaemon</code> is an abstraction for a Cassandra daemon
 * service, which defines not only a way to activate and deactivate it, but also
 * hooks into its lifecycle methods (see {@link #setup()}, {@link #start()},
 * {@link #stop()} and {@link #setup()}).
 */

/*
1, 找到Main方法了。
2. 以后可以从这开始往下再搞。
 */
public class CassandraDaemon //wxc 2015-9-14:16:44:49 Main方法放到Daemon类里很合适。
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=NativeAccess";//wxc 2015-8-7:12:54:44  JMX埋点
    private static JMXConnectorServer jmxServer = null;//wxc 2015-8-7:12:59:54 这个类第一次见。

    private static final Logger logger;
    static {
        // Need to register metrics before instrumented appender is created(first access to LoggerFactory).
        SharedMetricRegistries.getOrCreate("logback-metrics").addListener(new MetricRegistryListener.Base() //wxc pro 2015-8-7:13:01:49 这个SharedMetricRegistries类似于Zookeeper？ 从github上搜了下，看到这样的描述： Capturing JVM- and application-level metrics. So you know what's going on。 http://metrics.dropwizard.io/3.1.0/ 这个功能跟JMX重叠？
        {
            @Override
            public void onMeterAdded(String metricName, Meter meter)
            {
                // Given metricName consists of appender name in logback.xml + "." + metric name.
                // We first separate appender name
                int separator = metricName.lastIndexOf('.');
                String appenderName = metricName.substring(0, separator);
                String metric = metricName.substring(separator + 1); // remove "."
                ObjectName name = DefaultNameFactory.createMetricName(appenderName, metric, null).getMBeanName();
                CassandraMetricsRegistry.Metrics.registerMBean(meter, name);
            }
        });
        logger = LoggerFactory.getLogger(CassandraDaemon.class);
    }

    //wxc pro 2015-8-7:13:17:14 名字上加个maybe具体什么意思？
    private static void maybeInitJmx()
    {
        if (System.getProperty("com.sun.management.jmxremote.port") != null)
            return;

        String jmxPort = System.getProperty("cassandra.jmx.local.port");
        if (jmxPort == null)
            return;

        System.setProperty("java.rmi.server.hostname", InetAddress.getLoopbackAddress().getHostAddress());
        RMIServerSocketFactory serverFactory = new RMIServerSocketFactoryImpl();
        Map<String, ?> env = Collections.singletonMap(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, serverFactory);
        try
        {
            LocateRegistry.createRegistry(Integer.valueOf(jmxPort), null, serverFactory);
            JMXServiceURL url = new JMXServiceURL(String.format("service:jmx:rmi://localhost/jndi/rmi://localhost:%s/jmxrmi", jmxPort));
            jmxServer = new RMIConnectorServer(url, env, ManagementFactory.getPlatformMBeanServer());
            jmxServer.start();
        }
        catch (IOException e)
        {
            logger.error("Error starting local jmx server: ", e);
        }
    }

    private static final CassandraDaemon instance = new CassandraDaemon();//wxc 2015-8-7:13:07:23 也是类加载时就初始化了实例。

    public Server thriftServer;//wxc pro 2015-8-7:13:07:51 这个thriftServer没有Cassandra里？它跟CassandraDaemon怎么通信？
    public Server nativeServer;//wxc pro 2015-8-7:13:08:38 这是个什么？ 2015-8-8 18:38:11 后来看到这个nativeServer并不是JVM里的native的意思， 而Cassandra里native的意思， 相对thrift而言的。 这样通了。 看来这个server是专门用来接收请求的。

    private final boolean runManaged;//wxc pro 2015-8-7:13:08:55 标识着什么？
    protected final StartupChecks startupChecks;//wxc pro 2015-8-7:13:09:06 校验配置项？
    private boolean setupCompleted;

    public CassandraDaemon() {
        this(false);
    }

    public CassandraDaemon(boolean runManaged) {
        this.runManaged = runManaged;
        this.startupChecks = new StartupChecks().withDefaultTests();
        this.setupCompleted = false;
    }

    /**
     * This is a hook for concrete daemons to initialize themselves suitably.
     *
     * Subclasses should override this to finish the job (listening on ports, etc.)
     */
    //wxc 2015-9-16:9:03:20 看这里的描述， 对override有了形象的理解：所谓override， 就在骑在别人的马背上。 直接使用，这样最终在细胞层面上区分出Overwrite。overwrite的字面意思几乎是推倒重来。
    protected void setup()
    {
        // Delete any failed snapshot deletions on Windows - see CASSANDRA-9658
        if (FBUtilities.isWindows()) //wxc 2015-8-15:21:37:22 什么东西只有Windows下才有？ 另外是不是对Windows的判断是不是可以优雅些？ //wxc 2015-9-16:9:09:47 回看前个时间点的注释：没有整体业务性，而只是“挑刺”。现在回到业务的上正道上来后， 再看，这不是个问题。
            WindowsFailedSnapshotTracker.deleteOldSnapshots();

        ThreadAwareSecurityManager.install();//wxc pro 2015-8-7:13:15:26 这个类ThreadAwareSecurityManager能搞啥？

        logSystemInfo();

        CLibrary.tryMlockall();//wxc pro 2015-8-7:13:16:29 从这看是有C语言相关的东西？ 结合前面的Native字样， 貌似。

        try
        {
            startupChecks.verify();
        }
        catch (StartupException e)
        {
            exitOrFail(e.returnCode, e.getMessage(), e.getCause()); //wxc 2015-9-16:9:12:15 看到returnCode的定义， 考虑太细致了， 也
        }

        try
        {
            if (SystemKeyspace.snapshotOnVersionChange())
            {
                SystemKeyspace.migrateDataDirs();
            }
        }
        catch (IOException e)
        {
            exitOrFail(3, e.getMessage(), e.getCause());
        }

        maybeInitJmx();

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() //wxc 2015-8-7:13:18:02 第一次见定制这个ExceptionHandler的。
        {
            public void uncaughtException(Thread t, Throwable e)
            {
                StorageMetrics.exceptions.inc();
                logger.error("Exception in thread {}", t, e);
                Tracing.trace("Exception in thread {}", t, e);
                for (Throwable e2 = e; e2 != null; e2 = e2.getCause())
                {
                    JVMStabilityInspector.inspectThrowable(e2);

                    if (e2 instanceof FSError)
                    {
                        if (e2 != e) // make sure FSError gets logged exactly once.
                            logger.error("Exception in thread {}", t, e2);
                        FileUtils.handleFSError((FSError) e2);
                    }

                    if (e2 instanceof CorruptSSTableException)
                    {
                        if (e2 != e)
                            logger.error("Exception in thread " + t, e2);
                        FileUtils.handleCorruptSSTable((CorruptSSTableException) e2);
                    }
                }
            }
        });

        /*
         * Migrate pre-3.0 keyspaces, tables, types, functions, and aggregates, to their new 3.0 storage.
         * We don't (and can't) wait for commit log replay here, but we don't need to - all schema changes force
         * explicit memtable flushes.
         */
        LegacySchemaMigrator.migrate();

        StorageService.instance.populateTokenMetadata();

        // load schema from disk
        Schema.instance.loadFromDisk();//wxc 2015-8-7:13:20:21 第一次看到Schema相关的东西了。

        // clean up debris in the rest of the keyspaces //wxc pro 2015-8-7:13:22:22 debris指？
        for (String keyspaceName : Schema.instance.getKeyspaces())
        {
            // Skip system as we've already cleaned it
            if (keyspaceName.equals(SystemKeyspace.NAME))
                continue;

            for (CFMetaData cfm : Schema.instance.getTables(keyspaceName))//wxc 2015-8-7:13:22:56 像这样的庞大的工程， 出于什么考虑没有用Spring这类的IoC？
                ColumnFamilyStore.scrubDataDirectories(cfm);
        }

        Keyspace.setInitialized();//wxc pro 2015-8-7:13:23:59 感觉这个Key是列存储里很重要的一个概念。
        // initialize keyspaces
        for (String keyspaceName : Schema.instance.getKeyspaces())
        {
            if (logger.isDebugEnabled())
                logger.debug("opening keyspace {}", keyspaceName);
            // disable auto compaction until commit log replay ends
            for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
            {
                for (ColumnFamilyStore store : cfs.concatWithIndexes())
                {
                    store.disableAutoCompaction();
                }
            }
        }

        //wxc pro 2015-8-7:13:29:14 Cache是按key和row来细分的？
        if (CacheService.instance.keyCache.size() > 0)//wxc 2015-8-7:13:25:02 也是自己设计的Cache？
            logger.info("completed pre-loading ({} keys) key cache.", CacheService.instance.keyCache.size());

        if (CacheService.instance.rowCache.size() > 0)
            logger.info("completed pre-loading ({} keys) row cache.", CacheService.instance.rowCache.size());

        try
        {
            GCInspector.register();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn("Unable to start GCInspector (currently only supported on the Sun JVM)");
        }

        // replay the log if necessary
        try
        {
            CommitLog.instance.recover();//wxc pro 2015-8-7:13:32:34 这个是处理上一次意外死机的数据丢失问题？ 一次性run， 并没有worker在跑
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        // enable auto compaction
        for (Keyspace keyspace : Keyspace.all())
        {
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                for (final ColumnFamilyStore store : cfs.concatWithIndexes())
                {
                    if (store.getCompactionStrategyManager().shouldBeEnabled())
                        store.enableAutoCompaction();
                }
            }
        }

        Runnable indexRebuild = new Runnable()//wxc pro 2015-8-7:13:44:31 从这开始可以看出DB里建索引的相关逻辑？
        {
            @Override
            public void run()
            {
                for (Keyspace keyspace : Keyspace.all())
                {
                    for (ColumnFamilyStore cf: keyspace.getColumnFamilyStores())
                    {
                        cf.materializedViewManager.buildAllViews();
                    }
                }
            }
        };

        ScheduledExecutors.optionalTasks.schedule(indexRebuild, StorageService.RING_DELAY, TimeUnit.MILLISECONDS);


        SystemKeyspace.finishStartup();  //wxc 2015-8-7:20:34:46 放这个到一个单独的方法里貌似更合适。

        // start server internals
        StorageService.instance.registerDaemon(this);//wxc 2015-8-8:8:47:54 又一个重量级角色上场了。
        try
        {
            StorageService.instance.initServer();
        }
        catch (ConfigurationException e)
        {
            System.err.println(e.getMessage() + "\nFatal configuration error; unable to start server.  See log for stacktrace.");
            exitOrFail(1, "Fatal configuration error", e);
        }

        Mx4jTool.maybeLoad();//wxc pro 2015-8-8:9:06:56  Mx4j是个什么东东？

        // Metrics
        String metricsReporterConfigFile = System.getProperty("cassandra.metricsReporterConfigFile");
        if (metricsReporterConfigFile != null)
        {
            logger.info("Trying to load metrics-reporter-config from file: {}", metricsReporterConfigFile);
            try
            {
                String reportFileLocation = CassandraDaemon.class.getClassLoader().getResource(metricsReporterConfigFile).getFile();
                ReporterConfig.loadFromFile(reportFileLocation).enableAll(CassandraMetricsRegistry.Metrics);
            }
            catch (Exception e)
            {
                logger.warn("Failed to load metrics-reporter-config, metric sinks will not be activated", e);
            }
        }

        if (!FBUtilities.getBroadcastAddress().equals(InetAddress.getLoopbackAddress()))
            waitForGossipToSettle();

        // schedule periodic background compaction task submission. this is simply a backstop against compactions stalling
        // due to scheduling errors or race conditions
        ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(ColumnFamilyStore.getBackgroundCompactionTaskSubmitter(), 5, 1, TimeUnit.MINUTES);

        // schedule periodic dumps of table size estimates into SystemKeyspace.SIZE_ESTIMATES_CF
        // set cassandra.size_recorder_interval to 0 to disable
        int sizeRecorderInterval = Integer.getInteger("cassandra.size_recorder_interval", 5 * 60);
        if (sizeRecorderInterval > 0)
            ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(SizeEstimatesRecorder.instance, 30, sizeRecorderInterval, TimeUnit.SECONDS);

        // Thrift
        InetAddress rpcAddr = DatabaseDescriptor.getRpcAddress();
        int rpcPort = DatabaseDescriptor.getRpcPort();
        int listenBacklog = DatabaseDescriptor.getRpcListenBacklog();
        thriftServer = new ThriftServer(rpcAddr, rpcPort, listenBacklog);

        // Native transport
        InetAddress nativeAddr = DatabaseDescriptor.getRpcAddress();
        int nativePort = DatabaseDescriptor.getNativeTransportPort();
        nativeServer = new org.apache.cassandra.transport.Server(nativeAddr, nativePort);

        //wxc pro 2015-8-8:9:13:21 上面的两个server是都开么？

        setupCompleted = true;
    }

    public boolean setupCompleted()
    {
        return setupCompleted;
    }

    private void logSystemInfo()
    {
    	if (logger.isInfoEnabled())
    	{
	        try
	        {
	            logger.info("Hostname: {}", InetAddress.getLocalHost().getHostName());
	        }
	        catch (UnknownHostException e1)
	        {
	            logger.info("Could not resolve local host");
	        }
	
	        logger.info("JVM vendor/version: {}/{}", System.getProperty("java.vm.name"), System.getProperty("java.version"));
	        logger.info("Heap size: {}/{}", Runtime.getRuntime().totalMemory(), Runtime.getRuntime().maxMemory());
	
	        for(MemoryPoolMXBean pool: ManagementFactory.getMemoryPoolMXBeans())
	            logger.info("{} {}: {}", pool.getName(), pool.getType(), pool.getPeakUsage());
	
	        logger.info("Classpath: {}", System.getProperty("java.class.path"));
    	}
    }

    /**
     * Initialize the Cassandra Daemon based on the given <a
     * href="http://commons.apache.org/daemon/jsvc.html">Commons
     * Daemon</a>-specific arguments. To clarify, this is a hook for JSVC.
     *
     * @param arguments
     *            the arguments passed in from JSVC
     * @throws IOException
     */
    //wxc pro 2015-8-7:13:14:07  commons Daemon 还是第一次见。
    //wxc pro 2015-8-7:13:14:35 怎么是JSVC？
    public void init(String[] arguments) throws IOException
    {
        setup();
    }

    /**
     * Start the Cassandra Daemon, assuming that it has already been
     * initialized via {@link #init(String[])}
     *
     * Hook for JSVC
     */
    public void start()
    {
        String nativeFlag = System.getProperty("cassandra.start_native_transport");
        if ((nativeFlag != null && Boolean.parseBoolean(nativeFlag)) || (nativeFlag == null && DatabaseDescriptor.startNativeTransport()))
        {
            nativeServer.start();
        }
        else
            logger.info("Not starting native transport as requested. Use JMX (StorageService->startNativeTransport()) or nodetool (enablebinary) to start it");

        String rpcFlag = System.getProperty("cassandra.start_rpc");
        if ((rpcFlag != null && Boolean.parseBoolean(rpcFlag)) || (rpcFlag == null && DatabaseDescriptor.startRpc()))
            thriftServer.start();
        else
            logger.info("Not starting RPC server as requested. Use JMX (StorageService->startRPCServer()) or nodetool (enablethrift) to start it");
    }

    /**
     * Stop the daemon, ideally in an idempotent manner.
     *
     * Hook for JSVC / Procrun
     */
    public void stop()
    {
        // On linux, this doesn't entirely shut down Cassandra, just the RPC server.
        // jsvc takes care of taking the rest down
        logger.info("Cassandra shutting down...");
        thriftServer.stop();
        nativeServer.stop();

        // On windows, we need to stop the entire system as prunsrv doesn't have the jsvc hooks
        // We rely on the shutdown hook to drain the node
        if (FBUtilities.isWindows())
            System.exit(0);

        if (jmxServer != null)
        {
            try
            {
                jmxServer.stop();
            }
            catch (IOException e)
            {
                logger.error("Error shutting down local JMX server: ", e);
            }
        }
    }


    /**
     * Clean up all resources obtained during the lifetime of the daemon. This
     * is a hook for JSVC.
     */
    public void destroy()
    {}

    /**
     * A convenience method to initialize and start the daemon in one shot.
     */
    public void activate()
    {
        String pidFile = System.getProperty("cassandra-pidfile");//wxc 2015-8-8:10:00:36 debug运行时发现没有pid文件。

        if (FBUtilities.isWindows())
        {
            // We need to adjust the system timer on windows from the default 15ms down to the minimum of 1ms as this
            // impacts timer intervals, thread scheduling, driver interrupts, etc.
            WindowsTimer.startTimerPeriod(DatabaseDescriptor.getWindowsTimerInterval());
        }

        try
        {
            try
            {
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); //wxc pro 2015-12-9:20:59:07 这种方式启动JMX相关服务？
                mbs.registerMBean(new StandardMBean(new NativeAccess(), NativeAccessMBean.class), new ObjectName(MBEAN_NAME));//wxc pro 2015-12-9:21:02:27 这样注册后， 后续怎么个用法？
            }
            catch (Exception e)
            {
                logger.error("error registering MBean {}", MBEAN_NAME, e);
                //Allow the server to start even if the bean can't be registered
            }

            try {
                DatabaseDescriptor.forceStaticInitialization();
            } catch (ExceptionInInitializerError e) {
                throw e.getCause();
            }

            setup();

            if (pidFile != null)
            {
                new File(pidFile).deleteOnExit();
            }

            if (System.getProperty("cassandra-foreground") == null)
            {
                System.out.close();
                System.err.close();
            }

            start();
        }
        catch (Throwable e)
        {
            boolean logStackTrace =
                    e instanceof ConfigurationException ? ((ConfigurationException)e).logStackTrace : true;

            System.out.println("Exception (" + e.getClass().getName() + ") encountered during startup: " + e.getMessage());

            if (logStackTrace)
            {
                if (runManaged)
                    logger.error("Exception encountered during startup", e);
                // try to warn user on stdout too, if we haven't already detached
                e.printStackTrace();
                exitOrFail(3, "Exception encountered during startup", e);
            }
            else
            {
                if (runManaged)
                    logger.error("Exception encountered during startup: {}", e.getMessage());
                // try to warn user on stdout too, if we haven't already detached
                System.err.println(e.getMessage());
                exitOrFail(3, "Exception encountered during startup: " + e.getMessage());
            }
        }
    }

    /**
     * A convenience method to stop and destroy the daemon in one shot.
     */
    public void deactivate()
    {
        stop();
        destroy();
        // completely shut down cassandra
        if(!runManaged) {
            System.exit(0);
        }
    }

    //wxc pro 2015-8-8:9:10:18 这个方法貌似是等待手动关闭其他同端口号的应用？
    private void waitForGossipToSettle()
    {
        int forceAfter = Integer.getInteger("cassandra.skip_wait_for_gossip_to_settle", -1);
        if (forceAfter == 0)
        {
            return;
        }
        final int GOSSIP_SETTLE_MIN_WAIT_MS = 5000;
        final int GOSSIP_SETTLE_POLL_INTERVAL_MS = 1000;
        final int GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED = 3;

        logger.info("Waiting for gossip to settle before accepting client requests...");
        Uninterruptibles.sleepUninterruptibly(GOSSIP_SETTLE_MIN_WAIT_MS, TimeUnit.MILLISECONDS);
        int totalPolls = 0;
        int numOkay = 0;
        int epSize = Gossiper.instance.getEndpointStates().size();
        while (numOkay < GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED)
        {
            Uninterruptibles.sleepUninterruptibly(GOSSIP_SETTLE_POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
            int currentSize = Gossiper.instance.getEndpointStates().size();
            totalPolls++;
            if (currentSize == epSize)
            {
                logger.debug("Gossip looks settled.");
                numOkay++;
            }
            else
            {
                logger.info("Gossip not settled after {} polls.", totalPolls);
                numOkay = 0;
            }
            epSize = currentSize;
            if (forceAfter > 0 && totalPolls > forceAfter)
            {
                logger.warn("Gossip not settled but startup forced by cassandra.skip_wait_for_gossip_to_settle. Gossip total polls: {}",
                            totalPolls);
                break;
            }
        }
        if (totalPolls > GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED)
            logger.info("Gossip settled after {} extra polls; proceeding", totalPolls - GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED);
        else
            logger.info("No gossip backlog; proceeding");
    }

    public static void stop(String[] args)
    {
        instance.deactivate();
    }

    public static void main(String[] args)
    {
        instance.activate();
    }

    private void exitOrFail(int code, String message) {
        exitOrFail(code, message, null);
    }

    private void exitOrFail(int code, String message, Throwable cause) {
            if(runManaged) {
                RuntimeException t = cause!=null ? new RuntimeException(message, cause) : new RuntimeException(message);
                throw t;
            }
            else {
                logger.error(message, cause);
                System.exit(code);
            }

        }

    static class NativeAccess implements NativeAccessMBean
    {
        public boolean isAvailable()
        {
            return CLibrary.jnaAvailable();
        }

        public boolean isMemoryLockable()
        {
            return CLibrary.jnaMemoryLockable();
        }
    }

    public interface Server
    {
        /**
         * Start the server.
         * This method shoud be able to restart a server stopped through stop().
         * Should throw a RuntimeException if the server cannot be started
         */
        public void start();

        /**
         * Stop the server.
         * This method should be able to stop server started through start().
         * Should throw a RuntimeException if the server cannot be stopped
         */
        public void stop();

        /**
         * Returns whether the server is currently running.
         */
        public boolean isRunning();
    }
}
