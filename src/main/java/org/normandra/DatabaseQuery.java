package org.normandra;

import java.util.Collection;

/**
 * a simple database query
 * <p>
 * Date: 4/5/14
 */
public interface DatabaseQuery<T> extends Iterable<T>
{
    /**
     * @return Returns the first item in the query result.
     */
    T first() throws NormandraException;

    /**
     * lists all items returned by query
     */
    Collection<T> list() throws NormandraException;

    /**
     * @return Returns the number of items returned by this query.
     */
    boolean empty() throws NormandraException;
}
