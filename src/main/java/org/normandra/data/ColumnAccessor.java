package org.normandra.data;

import org.normandra.EntitySession;
import org.normandra.NormandraException;

/**
 * an object capable of retrieving column data
 * <p/>
 * User: bowen
 * Date: 1/15/14
 */
public interface ColumnAccessor
{
    boolean isEmpty(Object entity) throws NormandraException;
    boolean isLoaded(Object entity) throws NormandraException;
    Object getValue(Object entity, EntitySession session) throws NormandraException;
    boolean setValue(Object entity, DataHolder value, EntitySession session) throws NormandraException;
}
