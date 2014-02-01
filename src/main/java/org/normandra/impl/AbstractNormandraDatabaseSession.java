package org.normandra.impl;

import org.normandra.NormandraDatabaseSession;
import org.normandra.NormandraException;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityMeta;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * a common database session
 * <p/>
 * User: bowen
 * Date: 2/1/14
 */
abstract public class AbstractNormandraDatabaseSession implements NormandraDatabaseSession
{
    private final Map<EntityMeta, Map<Object, Object>> cache = new TreeMap<>();

    private boolean closed = false;


    @Override
    public void close()
    {
        this.closed = true;
        this.clearCache();
    }


    public boolean isClosed()
    {
        return this.closed;
    }


    @Override
    public void clear() throws NormandraException
    {
        this.clearCache();
    }


    protected void clearCache()
    {
        this.cache.clear();
    }


    protected final <T> T findCached(final EntityMeta<T> meta, final Serializable key)
    {
        if (null == meta || null == key)
        {
            return null;
        }
        final ColumnMeta column = meta.getPartition();
        if (null == column)
        {
            return null;
        }
        final Map entities = this.cache.get(meta);
        if (null == entities)
        {
            return null;
        }
        return meta.getType().cast(entities.get(key));
    }


    protected final <T> boolean cacheEntity(final EntityMeta<T> meta, final T obj) throws NormandraException
    {
        if (null == meta || null == obj)
        {
            return false;
        }

        Map<Object, Object> entities = this.cache.get(meta);
        if (null == entities)
        {
            entities = new HashMap<>();
            this.cache.put(meta, entities);
        }

        final ColumnMeta column = meta.getPartition();
        if (null == column)
        {
            return false;
        }
        final Object key = column.getAccessor().getValue(obj);
        if (null == key)
        {
            return false;
        }
        entities.put(key, obj);
        return true;
    }
}
