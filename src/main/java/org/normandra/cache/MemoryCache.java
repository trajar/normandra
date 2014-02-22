package org.normandra.cache;

import org.normandra.NormandraException;
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
    public <T> T get(final EntityMeta<T> meta, final Serializable key)
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
    public <T> boolean put(final EntityMeta<T> meta, final T instance)
    {
        if (null == meta || null == instance)
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
        catch (final NormandraException e)
        {
            logger.warn("Unable to retrieve key to cache entity [" + instance + "] of type [" + meta + "].", e);
            return false;
        }
    }
}
