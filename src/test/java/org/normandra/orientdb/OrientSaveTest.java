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
 * unit test to test persistence
 * <p/>
 * 
 * Date: 1/20/14
 */
public class OrientSaveTest
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
        helper.cleanup();
    }


    @Test
    public void testSimple() throws Exception
    {
        final OrientDatabase database = helper.getDatabase();
        final OrientDatabaseSession session = helper.getSession();
        final Map<Class, EntityMeta> entityMap = TestUtils.refresh(database, SimpleEntity.class);

        final EntityMeta meta = entityMap.values().iterator().next();
        final SimpleEntity entity = new SimpleEntity("test", Arrays.asList("foo", "bar"));
        session.save(meta, entity);
        Assert.assertEquals(1, entity.getId());
        Assert.assertEquals(2, session.listActivity().size());

        Assert.assertFalse(session.exists(meta, 0L));
        Assert.assertTrue(session.exists(meta, 1L));
        Assert.assertEquals(4, session.listActivity().size());

        final SimpleEntity notfound = (SimpleEntity) session.get(meta, 0L);
        Assert.assertNull(notfound);
        Assert.assertEquals(5, session.listActivity().size());
        final SimpleEntity existing = (SimpleEntity) session.get(meta, 1L);
        Assert.assertNotNull(existing);
        Assert.assertEquals(1, existing.getId());
        Assert.assertEquals(5, session.listActivity().size());

        session.delete(meta, existing);
        Assert.assertEquals(7, session.listActivity().size());
        Assert.assertFalse(session.exists(meta, 1L));
        Assert.assertEquals(8, session.listActivity().size());
        Assert.assertNull(session.get(meta, 1L));
        Assert.assertEquals(9, session.listActivity().size());
    }
}
