package org.normandra.association;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.EntitySession;
import org.normandra.NormandraException;
import org.normandra.meta.EntityContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * an element factory that unpacks entity ids from object instances
 * <p>
 * User: bowen
 * Date: 9/23/14
 */
public class BasicElementFactory<T> implements ElementFactory<T>
{
    private final EntityContext entity;


    public BasicElementFactory(final EntityContext entity)
    {
        if (null == entity)
        {
            throw new NullArgumentException("entity context");
        }
        this.entity = entity;
    }


    public EntityContext getEntity()
    {
        return entity;
    }


    @Override
    public Object pack(EntitySession session, final T value) throws NormandraException
    {
        return this.entity.getId().fromEntity(value);
    }


    @Override
    public List<?> pack(EntitySession session, final T... values) throws NormandraException
    {
        if (null == values || values.length <= 0)
        {
            return Collections.emptyList();
        }
        final List list = new ArrayList<>(values.length);
        for (final Object value : values)
        {
            final Object key = this.entity.getId().fromEntity(value);
            if (key != null)
            {
                list.add(key);
            }
        }
        return Collections.unmodifiableList(list);
    }


    @Override
    public T unpack(final EntitySession session, final Object value) throws NormandraException
    {
        return (T) session.get(this.entity, value);
    }


    @Override
    public List unpack(final EntitySession session, final Object... values) throws NormandraException
    {
        return session.get(this.entity, values);
    }
}
