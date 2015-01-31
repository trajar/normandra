package org.normandra.orientdb;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.normandra.TestUtils;
import org.normandra.entities.CatEntity;
import org.normandra.entities.DogEntity;
import org.normandra.entities.SimpleEntity;
import org.normandra.entities.ZooEntity;
import org.normandra.meta.EntityMeta;

import java.util.Arrays;
import java.util.Map;

/**
 * unit tests for orient index
 * <p/>
 * User: bowen
 * Date: 5/24/14
 */
public class OrientIndexTest
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
    public void destroy() throws Exception
    {
        helper.destroy();
    }


    @Test
    public void testSimple() throws Exception
    {
        final OrientDatabase database = helper.getDatabase();
        final OrientDatabaseSession session = helper.getSession();
        final Map<Class, EntityMeta> entityMap = TestUtils.refresh(database, SimpleEntity.class);

        final EntityMeta entity = entityMap.get(SimpleEntity.class);

        final SimpleEntity instance = new SimpleEntity("test", Arrays.asList("foo", "bar"));
        session.save(entity, instance);

        Assert.assertEquals(1, session.query("select from simple_entity where id = ?", Arrays.asList(1L)).size());
        Assert.assertEquals(1, session.query("select from simple_entity where id > ?", Arrays.asList(0L)).size());
        Assert.assertEquals(0, session.query("select from simple_entity where id > ?", Arrays.asList(1L)).size());
        Assert.assertEquals(0, session.query("select from simple_entity where id = ?", Arrays.asList(2L)).size());

        Assert.assertEquals(1, session.query("select from index:simple_entity.id where key = ?", Arrays.asList(1L)).size());
        Assert.assertEquals(1, session.query("select from index:simple_entity.id where key > ?", Arrays.asList(0L)).size());
        Assert.assertEquals(0, session.query("select from index:simple_entity.id where key = ?", Arrays.asList(2L)).size());
        Assert.assertEquals(1, session.query("select from index:simple_entity.id where key in [?, ?, ?]", Arrays.asList(1L, 2L, 3L)).size());
    }
}
