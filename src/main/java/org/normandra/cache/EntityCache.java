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
    Object get(EntityMeta meta, Serializable key);
    boolean remove(EntityMeta meta, Serializable key);
    boolean remove(EntityMeta meta, Object entity);
    boolean put(EntityMeta meta, Object entity);
}
