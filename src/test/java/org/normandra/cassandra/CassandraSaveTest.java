package org.normandra.cassandra;

import junit.framework.Assert;
import org.junit.Test;
import org.normandra.meta.AnnotationParser;
import org.normandra.meta.CatEntity;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.DogEntity;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.SimpleEntity;

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
        final SimpleEntity entity = new SimpleEntity("test", Arrays.asList("foo", "bar"));
        this.database.save(entityMap.get(SimpleEntity.class), entity);
        Assert.assertEquals(1, entity.getId());
    }


    @Test
    public void testInherited() throws Exception
    {
        final Map<Class, EntityMeta> entityMap = this.setupEntities(DogEntity.class, CatEntity.class);
        final DogEntity dog = new DogEntity("fido", 12);
        this.database.save(entityMap.get(DogEntity.class), dog);
        Assert.assertEquals(Long.valueOf(1), dog.getId());
        final CatEntity cat = new CatEntity("hank", true);
        this.database.save(entityMap.get(CatEntity.class), cat);
        Assert.assertEquals(Long.valueOf(2), cat.getId());
    }
}
