package org.normandra.orientdb;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.normandra.entities.SimpleEntity;
import org.normandra.meta.AnnotationParser;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;

import java.io.IOException;
import java.util.Arrays;

/**
 * cassandra unit tests
 * <p/>
 * User: bowen
 * Date: 9/7/13
 */
public class OrientSchemaTest extends BaseOrientTest
{
    private OrientDatabase database;


    @Before
    public void create() throws Exception
    {
        this.database = new OrientDatabaseFactory(path, user, password, construction).create();
    }


    @After
    public void destroy() throws IOException
    {
        if (this.database != null)
        {
            this.database.close();
            this.database = null;
        }
    }


    @Test
    public void testSimple() throws Exception
    {
        // we should start with clean database
        final AnnotationParser parser = new AnnotationParser(SimpleEntity.class);
        final EntityMeta entity = parser.read().iterator().next();
        Assert.assertNotNull(entity);
        Assert.assertEquals(9, this.database.getClusters().size());
        for (final TableMeta table : entity.getTables())
        {
            Assert.assertFalse(this.database.hasCluster(table.getName()));
        }

        // construct schema
        final DatabaseMeta meta = new DatabaseMeta(Arrays.asList(entity));
        final String clusterName = meta.getEntities().iterator().next().getName();
        this.database.refresh(meta);
        Assert.assertTrue(this.database.hasCluster(clusterName));
        Assert.assertTrue(this.database.hasProperty(clusterName, "id"));
        Assert.assertTrue(this.database.hasProperty(clusterName, "name"));
        Assert.assertFalse(this.database.hasProperty(clusterName, "name_column"));
        Assert.assertTrue(this.database.hasProperty(clusterName, "values"));
        Assert.assertFalse(this.database.hasProperty(clusterName, "foo"));

        // refresh without error
        this.database.refresh(meta);
        Assert.assertTrue(this.database.hasCluster(clusterName));
        Assert.assertTrue(this.database.hasProperty(clusterName, "id"));
        Assert.assertTrue(this.database.hasProperty(clusterName, "name"));
    }
}
