package org.normandra.cache;

import org.normandra.meta.EntityContext;
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
    Object get(EntityContext context, Serializable key);
    boolean remove(EntityMeta meta, Serializable key);
    boolean put(EntityMeta meta, Serializable key, Object entity);
    boolean put(EntityContext context, Serializable key, Object entity);
}
