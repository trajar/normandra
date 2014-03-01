package org.normandra;

import org.normandra.meta.DatabaseMeta;

/**
 * a NoSQL database abstraction
 * <p/>
 * User: bowen
 * Date: 8/31/13
 */
public interface Database
{
    DatabaseSession createSession() throws NormandraException;
    void refresh(DatabaseMeta meta) throws NormandraException;
    void close();
}
