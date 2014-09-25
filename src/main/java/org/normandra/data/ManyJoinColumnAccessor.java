package org.normandra.data;

import org.normandra.EntitySession;
import org.normandra.NormandraException;
import org.normandra.association.ElementFactory;
import org.normandra.association.LazyEntityList;
import org.normandra.association.LazyEntitySet;
import org.normandra.association.LazyLoadedCollection;
import org.normandra.meta.EntityContext;
import org.normandra.util.ArraySet;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
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
    private final ElementFactory factory;

    private final EntityContext entity;

    private final boolean lazy;


    public ManyJoinColumnAccessor(final Field field, final EntityContext meta, final boolean lazy, final ElementFactory factory)
    {
        super(field);
        this.factory = factory;
        this.entity = meta;
        this.lazy = lazy;
    }


    public EntityContext getEntity()
    {
        return entity;
    }


    public boolean isLazy()
    {
        return lazy;
    }


    @Override
    public boolean isLoaded(final Object entity) throws NormandraException
    {
        if (null == entity)
        {
            return false;
        }
        try
        {
            final Object value = this.get(entity);
            if (null == value)
            {
                return false;
            }
            if (!(value instanceof Collection))
            {
                return false;
            }
            if (value instanceof LazyLoadedCollection)
            {
                return ((LazyLoadedCollection) value).isLoaded();
            }
            else
            {
                return true;
            }
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to determine if many-join accessor is loaded.", e);
        }
    }


    protected final Collection<?> getCollection(final Object entity) throws IllegalAccessException, NoSuchMethodException, InvocationTargetException
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
    public Object getValue(final Object entity, EntitySession session) throws NormandraException
    {
        try
        {
            final Collection elements = this.getCollection(entity);
            if (null == elements)
            {
                return Collections.emptyList();
            }
            if (elements instanceof LazyLoadedCollection)
            {
                return ((LazyLoadedCollection) elements).duplicate();
            }
            if (elements.isEmpty())
            {
                return Collections.emptyList();
            }
            final List keys = this.factory.pack(session, this.getCollection(entity).toArray());
            if (Set.class.isAssignableFrom(this.getField().getType()))
            {
                return Collections.unmodifiableSet(new ArraySet(keys));
            }
            else
            {
                return Collections.unmodifiableList(keys);
            }
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get join-column collection.", e);
        }
    }


    @Override
    public boolean setValue(final Object entity, final DataHolder data, final EntitySession session) throws NormandraException
    {
        if (this.lazy)
        {
            try
            {
                // setup lazy loaded collection
                if (Set.class.isAssignableFrom(this.getField().getType()))
                {
                    return this.set(entity, new LazyEntitySet(session, this.entity, data, this.factory));
                }
                else
                {
                    return this.set(entity, new LazyEntityList(session, this.entity, data, this.factory));
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
