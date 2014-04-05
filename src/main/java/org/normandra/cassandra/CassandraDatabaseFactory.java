package org.normandra.cassandra;

import com.datastax.driver.core.Cluster;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.DatabaseConstruction;
import org.normandra.DatabaseFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * cassandra database factory
 * User: bowen
 * Date: 8/31/13
 */
public class CassandraDatabaseFactory implements DatabaseFactory
{
    public static final String DEFAULT_HOST = "localhost";

    public static final int DEFAULT_PORT = 9042;

    private final String host;

    private final int port;

    private final String keyspaceName;

    private final DatabaseConstruction mode;


    public CassandraDatabaseFactory(final String keyspace, final DatabaseConstruction mode)
    {
        this(keyspace, DEFAULT_HOST, DEFAULT_PORT, mode);
    }


    public CassandraDatabaseFactory(final String keyspace, final String host, final int port, final DatabaseConstruction mode)
    {
        if (null == keyspace)
        {
            throw new NullArgumentException("keyspace");
        }
        if (null == host)
        {
            throw new NullArgumentException("host");
        }
        if (null == mode)
        {
            throw new NullArgumentException("mode");
        }
        this.keyspaceName = keyspace;
        this.host = host;
        this.port = port;
        this.mode = mode;
    }


    @Override
    public CassandraDatabase create()
    {
        final Cluster cluster = this.buildCluster();
        final AtomicInteger counter = new AtomicInteger();
        final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory()
        {
            @Override
            public Thread newThread(final Runnable r)
            {
                final Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("CassandraWorker-" + counter.incrementAndGet());
                return thread;
            }
        });
        return new CassandraDatabase(this.keyspaceName, cluster, this.mode, executor);
    }


    private Cluster buildCluster()
    {
        final Cluster.Builder builder = Cluster.builder().addContactPoint(this.host);
//      builder.withSSL();
        if (this.port <= 0)
        {
            return builder.build();
        }
        return builder.withPort(this.port).build();
    }
}
