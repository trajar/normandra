package org.normandra.cache;

import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * an empty cache api that doesn't actually cache elements
 * <p>
 * <p>
 * 
 * Date: 7/14/14
 */
public class NullEntityCache implements EntityCache
{
    private static final EntityCache instance = new NullEntityCache();

    private static final EntityCacheFactory factory = new Factory();


    public static final EntityCache getInstance()
    {
        return NullEntityCache.instance;
    }


    public static final EntityCacheFactory getFactory()
    {
        return NullEntityCache.factory;
    }


    private static class Factory implements EntityCacheFactory
    {
        @Override
        public EntityCache create()
        {
            return NullEntityCache.instance;
        }
    }


    @Override
    public void clear()
    {

    }


    @Override
    public <T> Map<Object, T> find(EntityMeta meta, Collection<?> keys, Class<T> clazz)
    {
        return Collections.emptyMap();
    }


    @Override
    public <T> Map<Object, T> find(EntityContext context, Collection<?> keys, Class<T> clazz)
    {
        return Collections.emptyMap();
    }


    @Override
    public Object get(EntityMeta meta, Object key, Class clazz)
    {
        return null;
    }


    @Override
    public Object get(EntityContext context, Object key, Class clazz)
    {
        return null;
    }


    @Override
    public boolean remove(EntityMeta meta, Object key)
    {
        return false;
    }


    @Override
    public boolean put(EntityMeta meta, Object key, Object entity)
    {
        return false;
    }


    @Override
    public boolean put(EntityContext context, Object key, Object entity)
    {
        return false;
    }


    private NullEntityCache()
    {

    }
}
