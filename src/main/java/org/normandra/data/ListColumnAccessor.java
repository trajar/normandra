package org.normandra.data;

import org.normandra.NormandraException;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * concrete list element accessor
 * <p/>
 * User: bowen
 * Date: 1/19/14
 */
public class ListColumnAccessor extends CollectionColumnAccessor<List>
{
    public ListColumnAccessor(final Field prop, final Class<?> generic)
    {
        super(prop, generic);
    }


    @Override
    public List getValue(final Object entity) throws NormandraException
    {
        final Collection<?> list = this.getCollection(entity);
        return Collections.unmodifiableList(new ArrayList<>(list));
    }
}
