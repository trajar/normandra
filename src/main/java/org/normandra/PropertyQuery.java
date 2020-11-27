package org.normandra;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    default List<Map<String, Object>> list() throws NormandraException {
        final List<Map<String, Object>> list = new ArrayList<>();
        for (final Map<String, Object> item : this) {
            if (item != null) {
                list.add(item);
            }
        }
        return Collections.unmodifiableList(list);
    }

    /**
     * @return Returns the number of items returned by this query.
     */
    default boolean empty() throws NormandraException {
        return this.first() != null;
    }
}
