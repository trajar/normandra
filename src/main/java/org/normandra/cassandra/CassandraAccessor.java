package org.normandra.cassandra;

import com.datastax.driver.core.Session;

/**
 * simple session accessor api
 * <p/>
 * User: bowen
 * Date: 1/26/14
 */
public interface CassandraAccessor
{
    String getKeyspace();
    Session getSession();
}
