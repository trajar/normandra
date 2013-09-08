package org.normandra.cassandra;

import junit.framework.Assert;
import org.apache.log4j.BasicConfigurator;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.normandra.DatabaseConstruction;
import org.normandra.config.AnnotationParser;
import org.normandra.config.DatabaseMeta;
import org.normandra.config.EntityMeta;
import org.normandra.config.SimpleEntity;

import java.util.Arrays;

/**
 * cassandra unit tests
 * <p/>
 * User: bowen
 * Date: 9/7/13
 */
public class CassandraSchemaTest
{
    private CassandraDatabase database;


    @BeforeClass
    public static void setupCassandra() throws Exception
    {
        BasicConfigurator.configure();
//      Logger.getRootLogger().setLevel(Level.INFO);
        EmbeddedCassandraServerHelper.startEmbeddedCassandra("/cassandra-1.2.8.yaml");
    }


    @Before
    public void create()
    {
        this.database = new CassandraDatabaseFactory("test", "localhost", 9142, DatabaseConstruction.RECREATE).create();
    }


    @After
    public void destroy()
    {
        if (this.database != null)
        {
            this.database.close();
            this.database = null;
        }
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }


    @Test
    public void testSimple()
    {
        // we should start with clean keyspace
        final AnnotationParser parser = new AnnotationParser(SimpleEntity.class);
        final EntityMeta entity = parser.readEntity();
        final String table = entity.getTable();
        Assert.assertFalse(this.database.hasTable(table));
        Assert.assertNotNull(entity);

        // construct schema
        final DatabaseMeta meta = new DatabaseMeta(Arrays.asList(entity));
        this.database.refresh(meta);
        Assert.assertTrue(this.database.hasTable(table));
        Assert.assertTrue(this.database.hasColumn(table, "id"));
        Assert.assertFalse(this.database.hasColumn(table, "name"));
        Assert.assertTrue(this.database.hasColumn(table, "name_colum"));
        Assert.assertTrue(this.database.hasColumn(table, "values"));
        Assert.assertFalse(this.database.hasColumn(table, "foo"));

        // refresh without error
        this.database.refresh(meta);
        Assert.assertTrue(this.database.hasTable(table));
        Assert.assertTrue(this.database.hasColumn(table, "id"));
        Assert.assertTrue(this.database.hasColumn(table, "name_colum"));
        Assert.assertTrue(this.database.hasColumn(table, "values"));
    }
}
