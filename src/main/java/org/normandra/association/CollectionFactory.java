package org.normandra.association;

import java.util.Collection;

/**
 * a simple collection factory interface
 * <p>
 * 
 * Date: 3/30/14
 */
public interface CollectionFactory<T>
{
    Collection<T> create(int size);
}
