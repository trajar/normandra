package org.normandra.cassandra;

import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.normandra.DatabaseConstruction;

import java.io.IOException;

/**
 * common cassandra unit test bootstrap
 * <p/>
 * User: bowen
 * Date: 1/20/14
 */
public class BaseCassandraTest
{
    protected CassandraDatabase database;

    protected DatabaseConstruction construction = DatabaseConstruction.RECREATE;


    @BeforeClass
    public static void setupCassandra() throws Exception
    {
        BasicConfigurator.configure();
        CassandraUtil.start("/cassandra-2.0.0.yaml");
    }


    @Before
    public void create()
    {
        this.database = new CassandraDatabaseFactory("test", "localhost", 9142, this.construction).create();
    }


    @After
    public void destroy() throws IOException
    {
        if (this.database != null)
        {
            this.database.close();
            this.database = null;
        }
        CassandraUtil.reset();
    }
}
