package org.normandra;

import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityMeta;

/**
 * a NoSQL database abstraction
 * <p/>
 * User: bowen
 * Date: 8/31/13
 */
public interface NormandraDatabase
{
    <T> void save(EntityMeta meta, T element) throws NormandraException;
    void refresh(DatabaseMeta meta) throws NormandraException;
    void close();
}
