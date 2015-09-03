package org.normandra.orientdb;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.normandra.entities.CatEntity;
import org.normandra.entities.DogEntity;
import org.normandra.entities.SimpleEntity;
import org.normandra.entities.ZooEntity;
import org.normandra.meta.AnnotationParser;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;

import java.util.Arrays;

/**
 * cassandra unit tests
 * <p/>
 * User: bowen
 * Date: 9/7/13
 */
public class OrientSchemaTest
{
    private final OrientTestHelper helper = new OrientTestHelper();

    @BeforeClass
    public static void setup() throws Exception
    {
        OrientTestHelper.setup();
    }

    @Before
    public void create() throws Exception
    {
        helper.create(Arrays.asList(DogEntity.class, CatEntity.class, ZooEntity.class));
    }

    @After
    public void cleanup()
    {
        helper.cleanup();
    }

    @Test
    public void testSimple() throws Exception
    {
        // we should start with clean database
        final OrientDatabase database = helper.getDatabase();
        final AnnotationParser parser = new AnnotationParser(new OrientAccessorFactory(), SimpleEntity.class);
        final EntityMeta entity = parser.read().iterator().next();
        Assert.assertNotNull(entity);
        for (final TableMeta table : entity.getTables())
        {
            Assert.assertFalse(database.hasCluster(table.getName()));
        }

        // construct schema
        final DatabaseMeta meta = new DatabaseMeta(Arrays.asList(entity));
        database.refresh(meta);
        Assert.assertTrue(database.hasCluster("simple_entity"));
        Assert.assertTrue(database.hasProperty("simple_entity", "id"));
        Assert.assertFalse(database.hasProperty("simple_entity", "name"));
        Assert.assertTrue(database.hasProperty("simple_entity", "name_column"));
        Assert.assertTrue(database.hasProperty("simple_entity", "values"));
        Assert.assertFalse(database.hasProperty("simple_entity", "foo"));

        // refresh without error
        database.refresh(meta);
        Assert.assertTrue(database.hasCluster("simple_entity"));
        Assert.assertTrue(database.hasProperty("simple_entity", "id"));
        Assert.assertTrue(database.hasProperty("simple_entity", "name_column"));
    }
}
