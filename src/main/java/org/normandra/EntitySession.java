package org.normandra;

import org.normandra.meta.EntityMeta;

/**
 * a session api that is cable of retrieving entity instances
 * <p/>
 * User: bowen
 * Date: 2/9/14
 */
public interface EntitySession
{
    <T> boolean exists(EntityMeta<T> meta, Object key) throws NormandraException;
    <T> T get(EntityMeta<T> meta, Object key) throws NormandraException;
}
