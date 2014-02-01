package org.normandra;

import org.normandra.meta.DatabaseMeta;

/**
 * a NoSQL database abstraction
 * <p/>
 * User: bowen
 * Date: 8/31/13
 */
public interface NormandraDatabase
{
    NormandraDatabaseSession createSession() throws NormandraException;
    void refresh(DatabaseMeta meta) throws NormandraException;
    void close();
}
