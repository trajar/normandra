package org.normandra.cassandra;

import org.normandra.DatabaseConstruction;
import org.normandra.EntityManager;
import org.normandra.EntityManagerFactory;
import org.normandra.TestHelper;
import org.normandra.cache.EntityCacheFactory;
import org.normandra.cache.MapFactory;
import org.normandra.cache.MemoryCache;

import java.util.Collection;

/**
 * common cassandra unit test bootstrap
 * <p/>
 *  Date: 1/20/14
 */
public class CassandraTestHelper implements TestHelper
{
    public static final String keyspace = "test";

    public static final int port = 9142;

    public static final DatabaseConstruction construction = DatabaseConstruction.RECREATE;

    private final EntityCacheFactory cache = new MemoryCache.Factory(MapFactory.withConcurrency());

    private CassandraDatabase database;

    private CassandraDatabaseSession session;

    private EntityManagerFactory factory;

    private EntityManager manager;

    public static void setup() throws Exception
    {
        CassandraTestUtil.start("/cassandra-2.1.0.yaml");
    }

    @Override
    public EntityManagerFactory getFactory()
    {
        return this.factory;
    }

    @Override
    public EntityManager getManager()
    {
        return this.manager;
    }

    @Override
    public CassandraDatabase getDatabase()
    {
        return this.database;
    }

    @Override
    public CassandraDatabaseSession getSession()
    {
        return this.session;
    }

    @Override
    public void create(final Collection<Class> types) throws Exception
    {
        final EntityManagerFactory.Builder builder = new EntityManagerFactory.Builder()
            .withType(EntityManagerFactory.Type.CASSANDRA)
            .withParameter(CassandraDatabase.HOSTS, "localhost")
            .withParameter(CassandraDatabase.PORT, port)
            .withParameter(CassandraDatabase.KEYSPACE, keyspace)
            .withDatabaseConstruction(construction);
        for (final Class<?> clazz : types)
        {
            builder.withClass(clazz);
        }
        this.factory = builder.create();
        this.manager = this.factory.create();
        this.database = new CassandraDatabaseFactory(keyspace, "localhost", port, cache, construction).create();
        this.session = this.database.createSession();
    }

    @Override
    public void cleanup()
    {
        try
        {
            if (this.session != null)
            {
                CassandraTestUtil.reset();
                this.session.close();
                this.session = null;
            }
            if (this.manager != null)
            {
                this.manager.close();
                this.manager = null;
            }
            if (this.database != null)
            {
                this.database.close();
                this.database.shutdown();
                this.database = null;
            }            
        }
        catch (final Exception e)
        {
//          e.printStackTrace();
        }
    }
}
