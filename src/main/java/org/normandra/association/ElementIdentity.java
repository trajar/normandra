package org.normandra.association;

import org.normandra.EntitySession;
import org.normandra.NormandraException;

import java.util.List;

/**
 * a simple factory that helps lazy-loaded collections fromEntity/toEntity values
 * <p>
 * User: bowen
 * Date: 9/23/14
 */
public interface ElementIdentity<T>
{
    Object fromKey(EntitySession session, Object key) throws NormandraException;
    Object fromEntity(EntitySession session, T value) throws NormandraException;
    List<?> fromEntities(EntitySession session, T... values) throws NormandraException;
    T toEntity(EntitySession session, Object value) throws NormandraException;
    List<T> toEntities(EntitySession session, Object... values) throws NormandraException;
}
