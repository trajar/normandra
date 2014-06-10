package org.normandra.cache;

import org.normandra.meta.EntityMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * a simple in-memory cache
 * <p/>
 * User: bowen
 * Date: 2/3/14
 */
public class MemoryCache implements EntityCache
{
    private static final Logger logger = LoggerFactory.getLogger(MemoryCache.class);

    private final Map<EntityMeta, Map<Object, Object>> cache = new TreeMap<>();


    @Override
    public void clear()
    {
        this.cache.clear();
    }


    @Override
    public Object get(final EntityMeta meta, final Serializable key)
    {
        if (null == meta || null == key)
        {
            return null;
        }

        final Map entities = this.cache.get(meta);
        if (null == entities)
        {
            return null;
        }

        final Object existing = entities.get(key);
        if (null == existing)
        {
            return null;
        }

        return meta.getType().cast(existing);
    }


    @Override
    public boolean remove(final EntityMeta meta, final Serializable key)
    {
        if (null == meta || null == meta.getId() || null == key)
        {
            return false;
        }

        final Map<Object, Object> entities = this.cache.get(meta);
        if (null == entities)
        {
            return false;
        }

        return entities.remove(key) != null;
    }


    @Override
    public boolean remove(final EntityMeta meta, final Object instance)
    {
        if (null == meta || null == meta.getId() || null == instance)
        {
            return false;
        }

        final Map<Object, Object> entities = this.cache.get(meta);
        if (null == entities)
        {
            return false;
        }

        try
        {
            final Object key = meta.getId().fromEntity(instance);
            if (null == key)
            {
                return false;
            }
            return entities.remove(key) != null;
        }
        catch (final Exception e)
        {
            logger.warn("Unable to remove key from cache entity [" + instance + "] of type [" + meta + "].", e);
            return false;
        }
    }


    @Override
    public boolean put(final EntityMeta meta, final Object instance)
    {
        if (null == meta || null == meta.getId() || null == instance)
        {
            return false;
        }

        Map<Object, Object> entities = this.cache.get(meta);
        if (null == entities)
        {
            entities = new ConcurrentHashMap<>();
            this.cache.put(meta, entities);
        }

        try
        {
            final Object key = meta.getId().fromEntity(instance);
            if (null == key)
            {
                return false;
            }
            entities.put(key, instance);
            return true;
        }
        catch (final Exception e)
        {
            logger.warn("Unable to retrieve key to cache entity [" + instance + "] of type [" + meta + "].", e);
            return false;
        }
    }
}
