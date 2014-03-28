package org.normandra.data;

import org.normandra.NormandraException;

/**
 * User: bowen
 * Date: 3/24/14
 */
public interface DataHolder
{
    boolean isEmpty();
    Object get() throws NormandraException;
}
