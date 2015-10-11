package org.normandra.data;

import org.normandra.NormandraException;

/**
 * 
 * Date: 3/24/14
 */
public interface DataHolder
{
    boolean isEmpty();
    Object get() throws NormandraException;
}
