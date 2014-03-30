package org.normandra.association;

import org.normandra.EntitySession;
import org.normandra.data.DataHolder;
import org.normandra.meta.EntityMeta;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * a lazy entity collection
 * <p>
 * User: bowen
 * Date: 3/25/14
 */
public class LazyEntitySet extends LazyEntityCollection implements Set<Object>
{
    public LazyEntitySet(EntitySession session, EntityMeta meta, DataHolder data)
    {
        super(session, meta, data);
    }


    @Override
    protected Collection<Object> createCollection()
    {
        return new HashSet<>();
    }


    @Override
    public LazyEntitySet duplicate()
    {
        return new LazyEntitySet(this.session, this.entity, this.data);
    }
}
