package org.normandra.data;

import org.normandra.NormandraException;

/**
 * an object capable of retrieving column data
 * <p/>
 * User: bowen
 * Date: 1/15/14
 */
public interface ColumnAccessor<T>
{
    boolean isEmpty(Object entity) throws NormandraException;
    T getValue(Object entity) throws NormandraException;
    boolean setValue(Object entity, T value) throws NormandraException;
}
