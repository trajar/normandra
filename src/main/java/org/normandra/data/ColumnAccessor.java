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
    T getValue(Object entity) throws NormandraException;
}
