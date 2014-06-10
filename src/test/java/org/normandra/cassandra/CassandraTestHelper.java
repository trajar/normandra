package org.normandra.cassandra;

import org.apache.log4j.BasicConfigurator;
import org.normandra.DatabaseConstruction;
import org.normandra.EntityManager;
import org.normandra.EntityManagerFactory;
import org.normandra.TestHelper;
import org.normandra.entities.CatEntity;
import org.normandra.entities.DogEntity;
import org.normandra.entities.ZooEntity;

/**
 * common cassandra unit test bootstrap
 * <p/>
 * User: bowen
 * Date: 1/20/14
 */
public class CassandraTestHelper implements TestHelper
{
    public static final String keyspace = "test";

    public static final int port = 9142;

    public static final DatabaseConstruction construction = DatabaseConstruction.RECREATE;

    private CassandraDatabase database;

    private CassandraDatabaseSession session;

    private EntityManager manager;


    public static void setup() throws Exception
    {
        BasicConfigurator.configure();
        CassandraTestUtil.start("/cassandra-2.0.0.yaml");
    }


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
    public void create() throws Exception
    {
        final EntityManagerFactory factory = new EntityManagerFactory.Builder()
                .withClass(CatEntity.class)
                .withClass(DogEntity.class)
                .withClass(ZooEntity.class)
                .withType(EntityManagerFactory.Type.CASSANDRA)
                .withParameter(CassandraDatabase.HOSTS, "localhost")
                .withParameter(CassandraDatabase.PORT, port)
                .withParameter(CassandraDatabase.KEYSPACE, keyspace)
                .withDatabaseConstruction(construction)
                .create();
        this.manager = factory.create();
        this.database = new CassandraDatabaseFactory(keyspace, "localhost", port, construction).create();
        this.session = this.database.createSession();
    }


    @Override
    public void destroy() throws Exception
    {
        if (this.session != null)
        {
            this.session.close();
            this.session = null;
        }
        if (this.database != null)
        {
            this.database.close();
            this.database = null;
        }
        if (this.manager != null)
        {
            this.manager.close();
            this.manager = null;
        }
        CassandraTestUtil.reset();
    }
}
