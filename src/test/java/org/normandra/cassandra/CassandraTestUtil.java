package org.normandra.cassandra;

import com.datastax.driver.core.Cluster;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.normandra.DatabaseConstruction;
import org.normandra.cache.NullEntityCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * unit test utilities
 * <p>
 * 
 * Date: 11/1/13
 */
public class CassandraTestUtil
{
    public static final int cqlPort = 8042;

    public static final int thriftPort = 8160;

    private static EmbeddedCassandraService embedded = null;

    private static final Logger logger = LoggerFactory.getLogger(CassandraTestUtil.class);


    public static void start() throws Exception
    {
        start("/cassandra.yaml");
    }


    public static void start(final String yamlFile) throws Exception
    {
        if (embedded != null)
        {
            return;
        }

        log4j(false);

        final File embeddedDir = new File("target/embeddedCassandra").getCanonicalFile();
        final File tmpYaml = new File(embeddedDir, FilenameUtils.getName(yamlFile));
        if (tmpYaml.exists())
        {
            FileUtils.forceDelete(tmpYaml);
        }
        if (embeddedDir.exists())
        {
            FileUtils.deleteDirectory(embeddedDir);
        }
        FileUtils.copyURLToFile(CassandraTestUtil.class.getResource(yamlFile), tmpYaml);

        System.setProperty("cassandra.config", tmpYaml.getCanonicalFile().toURI().toString());
        System.setProperty("cassandra-foreground", "true");

        clearAndReset();

        if (null == embedded)
        {
            final AtomicBoolean started = new AtomicBoolean(false);
            embedded = new EmbeddedCassandraService();
            final Runnable worker = new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        embedded.start();
                        started.getAndSet(true);
                    }
                    catch (final Exception e)
                    {
                        logger.warn("Unable to start embedded cassandra server.", e);
                    }
                }
            };
            final Thread thread = new Thread(worker);
            thread.setDaemon(true);
            thread.setName(CassandraTestUtil.class.getSimpleName() + "[embedded]");
            thread.start();

            for (int i = 0; i < 60; i++)
            {
                if (started.get())
                {
                    break;
                }
                Thread.sleep(250);
            }
        }
    }


    public static void reset() throws IOException
    {
        dropKeyspaces();
//      clearAndReset();
    }


    private static void dropKeyspaces()
    {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final Cluster cluster = Cluster.builder()
            .addContactPoint("localhost")
            .withPort(CassandraTestHelper.port).build();
        final CassandraDatabase db = new CassandraDatabase(CassandraTestHelper.keyspace, cluster, NullEntityCache.getFactory(), DatabaseConstruction.RECREATE, executor);
        try
        {
            for (final String name : Arrays.asList("TestCluster", "Test Cluster"))
            {
                if (db.hasKeyspace(name))
                {
                    db.dropKeyspace(name);
                }
            }
        }
        catch (final Exception e)
        {
            logger.warn("Unable to drop keyspace.", e);
        }
        finally
        {
            db.close();
            executor.shutdown();
        }
    }


    private static void clearAndReset() throws IOException
    {
        clear();
        DatabaseDescriptor.createAllDirectories();
        CommitLog.instance.resetUnsafe(true);
    }


    private static void clear() throws IOException
    {
        final String[] directoryNames = {DatabaseDescriptor.getCommitLogLocation(),};
        for (final String dirName : directoryNames)
        {
            final File dir = new File(dirName).getCanonicalFile();
            logger.info("Creating commit log at [" + dir + "].");
            if (dir.exists())
            {
                FileUtils.deleteDirectory(dir);
            }
        }

        for (final String dirName : DatabaseDescriptor.getAllDataFileLocations())
        {
            final File dir = new File(dirName).getCanonicalFile();
            logger.info("Creating data location at [" + dir + "].");
            if (dir.exists())
            {
                FileUtils.deleteDirectory(dir);
            }
        }
    }


    private static void log4j(final boolean resetAppenders)
    {
        /*
        if (resetAppenders)
        {
            final ConsoleAppender console = new ConsoleAppender(); //create appender
            final String PATTERN = "%d [%p|%c|%C{1}] %m%n";
            console.setLayout(new PatternLayout(PATTERN));
            console.setThreshold(org.apache.log4j.Level.WARN);
            console.activateOptions();
            org.apache.log4j.Logger.getRootLogger().getLoggerRepository().resetConfiguration();
            org.apache.log4j.Logger.getRootLogger().addAppender(console);
        }
        org.apache.log4j..getRootLogger().setLevel(org.apache.log4j.Level.WARN);
        org.apache.log4j.Logger.getLogger("org.apache.cassandra.db.Memtable").setLevel(org.apache.log4j.Level.OFF);
        java.util.logging.Logger.getGlobal().setLevel(java.util.logging.Level.WARNING);
        java.util.logging.Logger.getAnonymousLogger().setLevel(java.util.logging.Level.WARNING);
        java.util.logging.Logger.getLogger("org.apache.cassandra.db.Memtable").setLevel(java.util.logging.Level.OFF);
        */
    }


    private CassandraTestUtil()
    {
    }
}
