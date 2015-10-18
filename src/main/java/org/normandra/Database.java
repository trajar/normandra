package org.normandra;

import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityContext;

/**
 * a NoSQL database abstraction
 * <p/>
 * 
 * Date: 8/31/13
 */
public interface Database
{
    DatabaseSession createSession();
    void refresh(DatabaseMeta meta) throws NormandraException;
    boolean registerQuery(EntityContext meta, String name, String query) throws NormandraException;
    boolean unregisterQuery(String name) throws NormandraException;
    void close();
    void shutdown();
}
