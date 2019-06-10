package org.normandra;

import java.util.Collection;
import java.util.Map;

/**
 * a simple query that returns properties, rather than entities
 */
public interface PropertyQuery extends Iterable<Map<String, Object>>, AutoCloseable {
    /**
     * @return Returns the first item in the query result.
     */
    Map<String, Object> first() throws NormandraException;

    /**
     * lists all items returned by query
     */
    Collection<Map<String, Object>> list() throws NormandraException;

    /**
     * @return Returns the number of items returned by this query.
     */
    boolean empty() throws NormandraException;
}
