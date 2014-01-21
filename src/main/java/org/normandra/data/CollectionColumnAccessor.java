package org.normandra.data;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.NormandraException;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * simple column accessor for non-entity types
 * <p/>
 * User: bowen
 * Date: 1/19/14
 */
abstract public class CollectionColumnAccessor<T extends Collection> extends FieldColumnAccessor<T>
{
    private final Class<?> generic;


    public CollectionColumnAccessor(final Field field, final Class<?> generic)
    {
        super(field);
        if (null == generic)
        {
            throw new NullArgumentException("generic");
        }
        this.generic = generic;
    }


    public Collection<T> getCollection(final Object entity) throws NormandraException
    {
        if (null == entity)
        {
            return Collections.emptyList();
        }
        try
        {
            final Object obj = this.get(entity);
            if (null == obj)
            {
                return null;
            }
            if (obj instanceof Collection)
            {
                return new ArrayList<T>((Collection) obj);
            }
            else
            {
                throw new IllegalStateException("Expected value of type collection, found [" + obj + "].");
            }
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get property [" + this.getField().getName() + "] from entity [" + entity + "].", e);
        }
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        CollectionColumnAccessor that = (CollectionColumnAccessor) o;

        if (generic != null ? !generic.equals(that.generic) : that.generic != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (generic != null ? generic.hashCode() : 0);
        return result;
    }
}
