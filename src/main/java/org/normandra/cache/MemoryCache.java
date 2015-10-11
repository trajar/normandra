package org.normandra.cache;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * a simple in-memory cache
 * <p>
 * 
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
    public <T> T get(final EntityMeta meta, final Object key, final Class<T> clazz)
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

        if (null == clazz || Object.class.equals(clazz))
        {
            return (T) existing;
        }

        try
        {
            return clazz.cast(existing);
        }
        catch (final ClassCastException e)
        {
            return null;
        }
    }


    @Override
    public <T> T get(final EntityContext context, final Object key, final Class<T> clazz)
    {
        if (null == context || null == key)
        {
            return null;
        }
        for (final EntityMeta meta : context.getEntities())
        {
            final T existing = this.get(meta, key, clazz);
            if (existing != null)
            {
                return existing;
            }
        }
        return null;
    }


    @Override
    public <T> Map<Object, T> find(final EntityMeta meta, final Collection<?> keys, final Class<T> clazz)
    {
        if (null == meta || null == keys || keys.isEmpty())
        {
            return Collections.emptyMap();
        }

        final Map<Object, T> map = new HashMap<>();
        for (final Object key : keys)
        {
            final T item = this.get(meta, key, clazz);
            if (item != null)
            {
                map.put(key, item);
            }
        }
        return Collections.unmodifiableMap(map);
    }


    @Override
    public <T> Map<Object, T> find(final EntityContext context, final Collection<?> keys, final Class<T> clazz)
    {
        if (null == context || null == keys || keys.isEmpty())
        {
            return Collections.emptyMap();
        }

        final Map<Object, T> map = new HashMap<>();
        for (final Object key : keys)
        {
            final T item = this.get(context, key, clazz);
            if (item != null)
            {
                map.put(key, item);
            }
        }
        return Collections.unmodifiableMap(map);
    }


    @Override
    public boolean remove(final EntityMeta meta, final Object key)
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
    public boolean put(final EntityMeta meta, final Object key, final Object instance)
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


    @Override
    public boolean put(final EntityContext context, final Object key, final Object instance)
    {
        if (null == context || null == key || null == instance)
        {
            return false;
        }

        boolean updated = false;
        for (final EntityMeta meta : context.getEntities())
        {
            updated |= this.put(meta, key, instance);
        }
        return updated;
    }
}
