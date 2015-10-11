package org.normandra;

import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;

import java.util.List;

/**
 * a session api that is cable of retrieving entity instances
 * <p/>
 * 
 * Date: 2/9/14
 */
public interface EntitySession
{
    boolean exists(EntityContext meta, Object key) throws NormandraException;
    boolean exists(EntityMeta meta, Object key) throws NormandraException;
    Object get(EntityContext meta, Object key) throws NormandraException;
    Object get(EntityMeta meta, Object key) throws NormandraException;
    List<Object> get(EntityContext meta, Object... keys) throws NormandraException;
    List<Object> get(EntityMeta meta, Object... keys) throws NormandraException;
}
