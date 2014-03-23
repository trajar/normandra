package org.normandra.data;

import org.normandra.NormandraException;

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
    public SetColumnAccessor(final Field prop, final Class<?> generic)
    {
        super(prop, generic);
    }


    @Override
    public Set getValue(final Object entity) throws NormandraException
    {
        final Collection<?> list = this.getCollection(entity);
        return Collections.unmodifiableSet(new HashSet<>(list));
    }
}
