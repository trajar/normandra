package org.normandra.association;

import org.normandra.NormandraException;

/**
 * an object that pulls the associated value for a given relationship
 * <p/>
 * User: bowen
 * Date: 2/2/14
 */
public interface AssociationAccessor
{
    Object get() throws NormandraException;
}
