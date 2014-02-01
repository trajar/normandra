package org.normandra.cassandra;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.normandra.meta.AnnotationParser;
import org.normandra.meta.CatEntity;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.DogEntity;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.SimpleEntity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * unit test to test persistence
 * <p/>
 * User: bowen
 * Date: 1/20/14
 */
public class CassandraSaveTest extends BaseCassandraTest
{
    private CassandraDatabase database;

    private CassandraDatabaseSession session;


    @Before
    public void create() throws Exception
    {
        this.database = new CassandraDatabaseFactory(keyspace, "localhost", port, construction).create();
        this.session = this.database.createSession();
    }


    @After
    public void destroy() throws IOException
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
        CassandraUtil.reset();
    }


    private Map<Class, EntityMeta> setupEntities(final Class... entities) throws Exception
    {
        final Map<Class, EntityMeta> map = new HashMap<>();
        final List<EntityMeta> list = new ArrayList<>();
        for (final Class<?> clazz : entities)
        {
            final AnnotationParser parser = new AnnotationParser(clazz);
            final EntityMeta entity = parser.readEntity();
            if (entity != null)
            {
                list.add(entity);
                map.put(clazz, entity);
            }
        }
        final DatabaseMeta meta = new DatabaseMeta(list);
        this.database.refresh(meta);
        return Collections.unmodifiableMap(map);
    }


    @Test
    public void testSimple() throws Exception
    {
        final Map<Class, EntityMeta> entityMap = this.setupEntities(SimpleEntity.class);
        final EntityMeta<SimpleEntity> meta = entityMap.values().iterator().next();
        final SimpleEntity entity = new SimpleEntity("test", Arrays.asList("foo", "bar"));
        this.session.save(meta, entity);
        Assert.assertEquals(1, entity.getId());

        Assert.assertFalse(this.session.exists(meta, 0));
        Assert.assertTrue(this.session.exists(meta, 1));

        final SimpleEntity notfound = this.session.get(meta, 0);
        Assert.assertNull(notfound);
        final SimpleEntity existing = this.session.get(meta, 1);
        Assert.assertNotNull(existing);
        Assert.assertEquals(1, existing.getId());

        this.session.delete(meta, existing);
        Assert.assertFalse(this.session.exists(meta, 1));
        Assert.assertNull(this.session.get(meta, 1));
    }


    @Test
    public void testInherited() throws Exception
    {
        final Map<Class, EntityMeta> entityMap = this.setupEntities(DogEntity.class, CatEntity.class);
        final DogEntity dog = new DogEntity("fido", 12);
        this.session.save(entityMap.get(DogEntity.class), dog);
        Assert.assertEquals(Long.valueOf(1), dog.getId());
        final CatEntity cat = new CatEntity("hank", true);
        this.session.save(entityMap.get(CatEntity.class), cat);
        Assert.assertEquals(Long.valueOf(2), cat.getId());
    }
}
