package org.normandra.association;

import java.util.Collection;

/**
 * 
 * Date: 3/29/14
 */
public interface LazyLoadedCollection<T> extends Collection<T>
{
    boolean isLoaded();
    LazyLoadedCollection<T> duplicate();
}
