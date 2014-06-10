package org.normandra.cassandra;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.normandra.Database;
import org.normandra.DatabaseSession;
import org.normandra.TestHelper;
import org.normandra.TestUtils;
import org.normandra.entities.SimpleEntity;
import org.normandra.meta.EntityMeta;

import java.util.Arrays;
import java.util.Map;

/**
 * unit test to test persistence
 * <p/>
 * User: bowen
 * Date: 1/20/14
 */
public class CassandraSaveTest
{
    private final TestHelper helper = new CassandraTestHelper();


    @BeforeClass
    public static void setup() throws Exception
    {
        CassandraTestHelper.setup();
    }


    @Before
    public void create() throws Exception
    {
        helper.create();
    }


    @After
    public void destroy() throws Exception
    {
        helper.destroy();
    }


    @Test
    public void testSimple() throws Exception
    {
        final Database database = helper.getDatabase();
        final DatabaseSession session = helper.getSession();
        final Map<Class, EntityMeta> entityMap = TestUtils.refresh(database, SimpleEntity.class);

        final EntityMeta meta = entityMap.values().iterator().next();
        final SimpleEntity entity = new SimpleEntity("test", Arrays.asList("foo", "bar"));
        session.save(meta, entity);
        Assert.assertEquals(1, entity.getId());
        Assert.assertEquals(1, session.listActivity().size());

        Assert.assertFalse(session.exists(meta, 0L));
        Assert.assertTrue(session.exists(meta, 1L));
        Assert.assertTrue(session.exists(meta, 1));
        Assert.assertEquals(4, session.listActivity().size());

        final SimpleEntity notfound = (SimpleEntity) session.get(meta, 0);
        Assert.assertNull(notfound);
        Assert.assertEquals(5, session.listActivity().size());
        final SimpleEntity existing = (SimpleEntity) session.get(meta, 1);
        Assert.assertNotNull(existing);
        Assert.assertEquals(1, existing.getId());
        Assert.assertEquals(6, session.listActivity().size());

        session.delete(meta, existing);
        Assert.assertEquals(7, session.listActivity().size());
        Assert.assertFalse(session.exists(meta, 1));
        Assert.assertEquals(8, session.listActivity().size());
        Assert.assertNull(session.get(meta, 1));
        Assert.assertEquals(9, session.listActivity().size());
    }
}
