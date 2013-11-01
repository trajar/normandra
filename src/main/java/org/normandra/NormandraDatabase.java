package org.normandra;

import org.normandra.config.DatabaseMeta;
import org.normandra.config.EntityMeta;

/**
 * a NoSQL database abstraction
 * <p/>
 * User: bowen
 * Date: 8/31/13
 */
public interface NormandraDatabase
{
    <T> void save(EntityMeta meta, T element);
    void refresh(DatabaseMeta meta);
    void close();
}
