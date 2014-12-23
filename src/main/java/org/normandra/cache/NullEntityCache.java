package org.normandra.cache;

import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;

import java.io.Serializable;

/**
 * an empty cache api that doesn't actually cache elements
 * <p>
 * <p>
 * User: bowen
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
    public Object get(EntityMeta meta, Serializable key)
    {
        return null;
    }


    @Override
    public Object get(EntityContext context, Serializable key)
    {
        return null;
    }


    @Override
    public boolean remove(EntityMeta meta, Serializable key)
    {
        return false;
    }


    @Override
    public boolean put(EntityMeta meta, Serializable key, Object entity)
    {
        return false;
    }


    @Override
    public boolean put(EntityContext context, Serializable key, Object entity)
    {
        return false;
    }


    private NullEntityCache()
    {
    }
}
