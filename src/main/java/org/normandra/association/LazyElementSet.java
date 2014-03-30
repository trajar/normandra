package org.normandra.association;

import org.normandra.EntitySession;
import org.normandra.data.DataHolder;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * an element collection set
 * <p>
 * User: bowen
 * Date: 3/30/14
 */
public class LazyElementSet<T> extends LazyElementCollection<T> implements Set<T>
{
    public LazyElementSet(EntitySession session, DataHolder data)
    {
        super(session, data, new CollectionFactory<T>()
        {
            @Override
            public Collection<T> create(int size)
            {
                return new HashSet<>(size);
            }
        });
    }


    @Override
    public LazyLoadedCollection<T> duplicate()
    {
        return new LazyElementSet<>(this.session, this.data);
    }
}
