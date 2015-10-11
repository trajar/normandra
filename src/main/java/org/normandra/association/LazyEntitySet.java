package org.normandra.association;

import org.normandra.EntitySession;
import org.normandra.data.DataHolder;
import org.normandra.meta.EntityContext;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * a lazy entity collection
 * <p>
 * 
 * Date: 3/25/14
 */
public class LazyEntitySet<T> extends LazyEntityCollection<T> implements Set<T>
{
    public LazyEntitySet(EntitySession session, EntityContext meta, DataHolder data, ElementIdentity<T> factory)
    {
        super(session, meta, data, factory, new CollectionFactory<T>()
        {
            @Override
            public Collection<T> create(int size)
            {
                return new HashSet<>(size);
            }
        });
    }


    @Override
    public LazyEntitySet<T> duplicate()
    {
        return new LazyEntitySet<>(this.session, this.entity, this.data, this.elementFactory);
    }
}
