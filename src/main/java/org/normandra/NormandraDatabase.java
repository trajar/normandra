package org.normandra;

import org.normandra.config.DatabaseMeta;

/**
 * a NoSQL database abstraction
 * <p/>
 * User: bowen
 * Date: 8/31/13
 */
public interface NormandraDatabase
{
    void refresh(DatabaseMeta meta);
    void close();
}
