package org.normandra.cassandra;

import org.apache.log4j.BasicConfigurator;
import org.junit.BeforeClass;
import org.normandra.DatabaseConstruction;

/**
 * common cassandra unit test bootstrap
 * <p/>
 * User: bowen
 * Date: 1/20/14
 */
public class BaseCassandraTest
{
    public static final String keyspace = "test";

    public static final int port = 9142;

    public static final DatabaseConstruction construction = DatabaseConstruction.RECREATE;


    @BeforeClass
    public static void setupCassandra() throws Exception
    {
        BasicConfigurator.configure();
        CassandraUtil.start("/cassandra-2.0.0.yaml");
    }
}
