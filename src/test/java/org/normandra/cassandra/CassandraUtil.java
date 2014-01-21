package org.normandra.cassandra;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * unit test utilities
 * <p/>
 * User: bowen
 * Date: 11/1/13
 */
public class CassandraUtil
{
    public static final int cqlPort = 8042;

    public static final int thriftPort = 8160;

    private static EmbeddedCassandraService embedded = null;

    private static final Logger logger = LoggerFactory.getLogger(CassandraUtil.class);


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
            tmpYaml.delete();
        }
        embeddedDir.mkdirs();
        FileUtils.copyURLToFile(CassandraUtil.class.getResource(yamlFile), tmpYaml);

        System.setProperty("cassandra.config", "file:" + tmpYaml.getCanonicalPath());
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
                        started.lazySet(true);
                    }
                    catch (final Exception e)
                    {
                        logger.warn("Unable to start embedded cassandra server.", e);
                    }
                }
            };
            final Thread thread = new Thread(worker);
            thread.setDaemon(true);
            thread.setName(CassandraUtil.class.getSimpleName() + "[embedded]");
            thread.start();

            for (int i = 0; i < 60; i++)
            {
                if (started.get())
                {
                    break;
                }
                Thread.sleep(1000);
            }
            if (!started.get())
            {
                throw new RuntimeException("Unable to successfully start cassandra server.");
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
        Cluster cluster = null;
        for (final String name : Arrays.asList("TestCluster", "TestCluster"))
        {
            cluster = HFactory.getCluster(name);
            if (cluster != null)
            {
                logger.info("Found cluster [" + name + "].");
                break;
            }
        }
        if (null == cluster)
        {
            final String host = DatabaseDescriptor.getRpcAddress().getHostName();
            final int port = DatabaseDescriptor.getRpcPort();
            logger.info("Creating new cluster [TestCluster] at " + host + ":" + port + ".");
            cluster = HFactory.getOrCreateCluster("TestCluster", new CassandraHostConfigurator(host + ":" + port));
        }

        final List<String> keep = Arrays.asList("system", "system_auth", "system_traces");
        for (final KeyspaceDefinition keyspaceDefinition : cluster.describeKeyspaces())
        {
            final String keyspaceName = keyspaceDefinition.getName();
            if (!keep.contains(keyspaceName))
            {
                cluster.dropKeyspace(keyspaceName);
            }
        }
    }


    private static void clearAndReset() throws IOException
    {
        clear();
        DatabaseDescriptor.createAllDirectories();
        CommitLog.instance.resetUnsafe();
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
        if (resetAppenders)
        {
            final ConsoleAppender console = new ConsoleAppender(); //create appender
            final String PATTERN = "%d [%p|%c|%C{1}] %m%n";
            console.setLayout(new PatternLayout(PATTERN));
            console.setThreshold(org.apache.log4j.Level.INFO);
            console.activateOptions();
            org.apache.log4j.Logger.getRootLogger().getLoggerRepository().resetConfiguration();
            org.apache.log4j.Logger.getRootLogger().addAppender(console);
        }
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.INFO);
        java.util.logging.Logger.getGlobal().setLevel(java.util.logging.Level.INFO);
        java.util.logging.Logger.getAnonymousLogger().setLevel(java.util.logging.Level.INFO);
    }


    private CassandraUtil()
    {
    }
}
