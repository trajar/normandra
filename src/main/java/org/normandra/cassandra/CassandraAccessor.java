package org.normandra.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * simple session accessor api
 * <p/>
 * 
 * Date: 1/26/14
 */
public interface CassandraAccessor
{
    String getKeyspace();
    Cluster getCluster();
    Session getSession();
}
