package org.normandra.cache;

import org.normandra.meta.EntityMeta;

import java.io.Serializable;

/**
 * entity cache api
 * <p/>
 * User: bowen
 * Date: 2/3/14
 */
public interface EntityCache
{
    void clear();
    <T> T get(EntityMeta<T> meta, Serializable key);
    <T> boolean put(EntityMeta<T> meta, T entity);
}
