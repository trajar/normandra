package org.normandra.cache;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.meta.EntityMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * a simple memory cache, where all keys/values are stored with strong references
 */
public class StrongMemoryCache implements EntityCache {
    private static final Logger logger = LoggerFactory.getLogger(StrongMemoryCache.class);

    private final Map<EntityMeta, Map<Object, Object>> cache = new TreeMap<>();

    private final MapFactory maps;

    public static class Factory implements EntityCacheFactory {
        private final MapFactory maps;

        public Factory(final MapFactory f) {
            if (null == f) {
                throw new NullArgumentException("map factory");
            }
            this.maps = f;
        }

        @Override
        public EntityCache create() {
            return new StrongMemoryCache(this.maps);
        }
    }

    public StrongMemoryCache(final MapFactory f) {
        if (null == f) {
            throw new NullArgumentException("map factory");
        }
        this.maps = f;
    }

    @Override
    public void clear() {
        this.cache.values().forEach(Map::clear);
        this.cache.clear();
    }

    @Override
    public void clearType(final EntityMeta meta) {
        if (null == meta) {
            return;
        }
        final Map<Object, Object> entities = this.cache.remove(meta);
        if (entities != null && !entities.isEmpty()) {
            entities.clear();
        }
    }

    @Override
    public <T> T get(final EntityMeta meta, final Object key, final Class<T> clazz) {
        if (null == meta || null == key) {
            return null;
        }

        final Map<Object, Object> entities = this.cache.get(meta);
        if (null == entities) {
            return null;
        }

        final Object instance = entities.get(key);
        if (null == instance) {
            entities.remove(key);
            return null;
        }

        if (null == clazz || Object.class.equals(clazz)) {
            return (T) instance;
        }

        try {
            return clazz.cast(instance);
        } catch (final ClassCastException e) {
            logger.warn("Unable to convert [" + instance + "] to type [" + clazz + "].");
            return null;
        }
    }

    @Override
    public <T> Map<Object, T> find(final EntityMeta meta, final Collection<?> keys, final Class<T> clazz) {
        if (null == meta || null == keys || keys.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<Object, T> map = new HashMap<>();
        for (final Object key : keys) {
            final T item = this.get(meta, key, clazz);
            if (item != null) {
                map.put(key, item);
            }
        }
        return Collections.unmodifiableMap(map);
    }

    @Override
    public <T> Iterable<T> listByType(final EntityMeta meta, final Class<T> clazz) {
        if (null == meta) {
            return Collections.emptyList();
        }

        final Map<?, ?> map = this.cache.get(meta);
        if (null == map || map.isEmpty()) {
            return Collections.emptyList();
        }
        
        final List items = new ArrayList<>(map.size());
        for (final Object item : map.values()) {
            if (null == clazz || Object.class.equals(clazz)) {
                items.add(item);
            } else {
                try {
                    items.add(clazz.cast(item));
                } catch (final ClassCastException e) {
                    logger.warn("Unable to convert [" + item + "] to type [" + clazz + "].");
                }
            }
        }
        return Collections.unmodifiableList(items);
    }

    @Override
    public boolean remove(final EntityMeta meta, final Object key) {
        if (null == meta || null == meta.getId() || null == key) {
            return false;
        }

        final Map<Object, Object> entities = this.cache.get(meta);
        if (null == entities) {
            return false;
        }

        if (entities.remove(key) != null) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean put(final EntityMeta meta, final Object key, final Object instance) {
        if (null == meta || null == key) {
            return false;
        }

        Map<Object, Object> entities = this.cache.get(meta);
        if (null == entities) {
            entities = this.maps.create();
            this.cache.put(meta, entities);
        }

        try {
            if (instance != null) {
                entities.put(key, instance);
                return true;
            } else {
                return entities.remove(key) != null;
            }
        } catch (final Exception e) {
            logger.warn("Unable to assign key [" + key + "] to cache entity [" + instance + "] of type [" + meta + "].", e);
            return false;
        }
    }
}
