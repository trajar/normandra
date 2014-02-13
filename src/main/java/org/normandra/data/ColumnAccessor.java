package org.normandra.data;

import org.normandra.NormandraDatabaseSession;
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
    Object getValue(Object entity) throws NormandraException;
    boolean setValue(Object entity, Object value, NormandraDatabaseSession session) throws NormandraException;
}
