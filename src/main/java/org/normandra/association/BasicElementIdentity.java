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
public class BasicElementIdentity<T> implements ElementIdentity<T>
{
    private final EntityContext entity;


    public BasicElementIdentity(final EntityContext entity)
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
    public Object fromKey(EntitySession session, Object key) throws NormandraException
    {
        return key;
    }


    @Override
    public Object fromEntity(EntitySession session, final T value) throws NormandraException
    {
        return this.entity.getId().fromEntity(value);
    }


    @Override
    public List<?> fromEntities(EntitySession session, final T... values) throws NormandraException
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
    public T toEntity(final EntitySession session, final Object value) throws NormandraException
    {
        return (T) session.get(this.entity, value);
    }


    @Override
    public List toEntities(final EntitySession session, final Object... values) throws NormandraException
    {
        return session.get(this.entity, values);
    }
}
