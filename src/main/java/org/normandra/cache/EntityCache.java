package org.normandra.cache;

import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;

import java.util.Collection;
import java.util.Map;

/**
 * entity cache api
 * <p/>
 * User: bowen
 * Date: 2/3/14
 */
public interface EntityCache
{
    void clear();
    <T> T get(EntityMeta meta, Object key, Class<T> clazz);
    <T> T get(EntityContext context, Object key, Class<T> clazz);
    <T> Map<Object, T> find(EntityMeta meta, Collection<?> keys, Class<T> clazz);
    <T> Map<Object, T> find(EntityContext context, Collection<?> keys, Class<T> clazz);
    boolean remove(EntityMeta meta, Object key);
    boolean put(EntityMeta meta, Object key, Object entity);
    boolean put(EntityContext context, Object key, Object entity);
}
