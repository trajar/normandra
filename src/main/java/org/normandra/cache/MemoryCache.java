package org.normandra.cache;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

/**
 * a simple in-memory cache
 * <p>
 * User: bowen
 * Date: 2/3/14
 */
public class MemoryCache implements EntityCache
{
    private static final Logger logger = LoggerFactory.getLogger(MemoryCache.class);

    private final Map<EntityMeta, Map<Object, Object>> cache = new TreeMap<>();

    private final MapFactory maps;

    public static class Factory implements EntityCacheFactory
    {
        private final MapFactory maps;


        public Factory(final MapFactory f)
        {
            if (null == f)
            {
                throw new NullArgumentException("map factory");
            }
            this.maps = f;
        }


        @Override
        public EntityCache create()
        {
            return new MemoryCache(this.maps);
        }
    }


    public MemoryCache(final MapFactory f)
    {
        if (null == f)
        {
            throw new NullArgumentException("map factory");
        }
        this.maps = f;
    }


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

//      return meta.getType().cast(existing);
        return existing;
    }


    @Override
    public Object get(final EntityContext context, final Serializable key)
    {
        if (null == context || null == key)
        {
            return null;
        }
        for (final EntityMeta meta : context.getEntities())
        {
            final Object existing = this.get(meta, key);
            if (existing != null)
            {
                return existing;
            }
        }
        return null;
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

        if (entities.remove(key) != null)
        {
            return true;
        }
        else
        {
            return false;
        }
    }


    @Override
    public boolean put(final EntityMeta meta, final Serializable key, final Object instance)
    {
        if (null == meta || null == key || null == instance)
        {
            return false;
        }

        Map<Object, Object> entities = this.cache.get(meta);
        if (null == entities)
        {
            entities = this.maps.create();
            this.cache.put(meta, entities);
        }

        try
        {
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
