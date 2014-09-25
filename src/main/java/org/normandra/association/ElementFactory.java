package org.normandra.association;

import org.normandra.EntitySession;
import org.normandra.NormandraException;

import java.util.List;

/**
 * a simple factory that helps lazy-loaded collections pack/unpack values
 * <p>
 * User: bowen
 * Date: 9/23/14
 */
public interface ElementFactory<T>
{
    Object pack(EntitySession session, T value) throws NormandraException;
    List<?> pack(EntitySession session, T... values) throws NormandraException;
    T unpack(EntitySession session, Object value) throws NormandraException;
    List<T> unpack(EntitySession session, Object... values) throws NormandraException;
}
