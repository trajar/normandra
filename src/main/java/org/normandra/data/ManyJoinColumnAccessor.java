package org.normandra.data;

import org.normandra.DatabaseSession;
import org.normandra.NormandraException;
import org.normandra.association.LazyEntityList;
import org.normandra.association.LazyEntitySet;
import org.normandra.meta.EntityMeta;
import org.normandra.util.ArraySet;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * a one-to-many or many-to-many collection accessor
 * <p>
 * User: bowen
 * Date: 3/20/14
 */
public class ManyJoinColumnAccessor extends FieldColumnAccessor implements ColumnAccessor
{
    private final EntityMeta entity;

    private final boolean lazy;


    public ManyJoinColumnAccessor(final Field field, final EntityMeta meta, final boolean lazy)
    {
        super(field);
        this.entity = meta;
        this.lazy = lazy;
    }


    @Override
    public boolean isLoaded(final Object entity) throws NormandraException
    {
        return true;
    }


    private Collection<?> getCollection(final Object entity) throws IllegalAccessException, NoSuchMethodException, InvocationTargetException
    {
        final Object obj = this.get(entity);
        if (null == obj)
        {
            return Collections.emptyList();
        }
        if (obj instanceof Collection)
        {
            return (Collection) obj;
        }
        else
        {
            return Collections.emptyList();
        }
    }


    @Override
    public boolean isEmpty(final Object entity) throws NormandraException
    {
        try
        {
            return this.getCollection(entity).isEmpty();
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to query join-column collection.", e);
        }
    }


    @Override
    public Object getValue(final Object entity) throws NormandraException
    {
        try
        {
            final Collection elements = this.getCollection(entity);
            if (null == elements || elements.isEmpty())
            {
                return Collections.emptyList();
            }
            final List set = new ArrayList(elements.size());
            for (final Object associated : this.getCollection(entity))
            {
                final Object key = this.entity.getId().fromEntity(associated);
                if (key != null)
                {
                    set.add(key);
                }
            }
            return Collections.unmodifiableCollection(set);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get join-column collection.", e);
        }
    }


    @Override
    public boolean setValue(final Object entity, final DataHolder data, final DatabaseSession session) throws NormandraException
    {
        if (this.lazy)
        {
            try
            {
                // setup lazy loaded collection
                if(Set.class.isAssignableFrom(this.getField().getType()))
                {
                    return this.set(entity, new LazyEntitySet(session, this.entity, data));
                }
                else
                {
                    return this.set(entity, new LazyEntityList(session, this.entity, data));
                }
            }
            catch (final Exception e)
            {
                throw new NormandraException("Unable to set lazy join-column collection from data holder [" + data + "].", e);
            }
        }
        else
        {
            // get data now, load entities
            final Object value = data.get();
            if (!(value instanceof Collection))
            {
                throw new NormandraException("Expect collection of primary key values for [" + this.getField() + "] but found [" + value + "].");
            }
            try
            {
                final Collection keys = (Collection) value;
                if (keys.isEmpty())
                {
                    this.set(entity, new ArraySet());
                }
                else
                {
                    final List<Object> associations = session.get(this.entity, keys.toArray());
                    if (null == associations || associations.isEmpty())
                    {
                        this.set(entity, new ArraySet());
                    }
                    else
                    {
                        this.set(entity, new ArraySet<>(associations));
                    }
                }
                return true;
            }
            catch (final Exception e)
            {
                throw new NormandraException("Unable to set join-column collection from value [" + value + "].", e);
            }
        }
    }
}
