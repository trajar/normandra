package org.normandra;

import java.util.Collection;

/**
 * a simple database query
 * <p>
 * User: bowen
 * Date: 4/5/14
 */
public interface DatabaseQuery<T> extends Iterable<T>
{
    /**
     * @return Returns the first item in the query result.
     */
    T first() throws NormandraException;


    /**
     * @return Returns the last item in the query result.
     */
    T last() throws NormandraException;


    /**
     * lists all items returned by query
     */
    Collection<T> list() throws NormandraException;


    /**
     * @return Returns the number of items returned by this query.
     */
    int size() throws NormandraException;


    /**
     * Selects a subset of the results associated with this query.
     */
    Collection<T> subset(int offset, int count) throws NormandraException;
}
