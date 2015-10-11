package org.normandra.data;

import org.normandra.EntitySession;
import org.normandra.NormandraException;
import org.normandra.association.LazyElementList;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * concrete list element accessor
 * <p>
 * 
 * Date: 1/19/14
 */
public class ListColumnAccessor extends CollectionColumnAccessor
{
    public ListColumnAccessor(final Field prop, final Class<?> generic, final boolean lazy)
    {
        super(prop, generic, lazy);
    }


    @Override
    public boolean setValue(final Object entity, final DataHolder data, final EntitySession session) throws NormandraException
    {
        if (this.isLazy())
        {
            return this.setCollection(entity, new LazyElementList(session, data));
        }
        else
        {
            final Object value = data.get();
            return this.setCollection(entity, (Collection) value);
        }
    }


    @Override
    public List getValue(final Object entity, EntitySession session) throws NormandraException
    {
        final Collection<?> list = this.getCollection(entity);
        return Collections.unmodifiableList(new ArrayList<>(list));
    }
}
