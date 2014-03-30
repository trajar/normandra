package org.normandra.data;

import org.normandra.DatabaseSession;
import org.normandra.NormandraException;
import org.normandra.association.LazyElementSet;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * concrete set element accessor
 * <p>
 * User: bowen
 * Date: 1/19/14
 */
public class SetColumnAccessor extends CollectionColumnAccessor
{
    public SetColumnAccessor(final Field prop, final Class<?> generic, final boolean lazy)
    {
        super(prop, generic, lazy);
    }


    @Override
    public boolean setValue(final Object entity, final DataHolder data, final DatabaseSession session) throws NormandraException
    {
        if (this.isLazy())
        {
            return this.setCollection(entity, new LazyElementSet(session, data));
        }
        else
        {
            final Object value = data.get();
            return this.setCollection(entity, (Collection) value);
        }
    }


    @Override
    public Set getValue(final Object entity) throws NormandraException
    {
        final Collection<?> list = this.getCollection(entity);
        return Collections.unmodifiableSet(new HashSet<>(list));
    }
}
