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

    default Object firstScalar() throws NormandraException {
        final Map<String, Object> map = this.first();
        if (null == map || map.isEmpty() || map.keySet().isEmpty()) {
            return null;
        }
        final String firstKey = map.keySet().iterator().next();
        return map.get(firstKey);
    }

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
