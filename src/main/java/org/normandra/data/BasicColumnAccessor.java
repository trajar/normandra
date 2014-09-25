package org.normandra.data;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.EntitySession;
import org.normandra.NormandraException;

import java.lang.reflect.Field;

/**
 * a generic column data accessor
 * <p>
 * User: bowen
 * Date: 1/15/14
 */
public class BasicColumnAccessor extends FieldColumnAccessor implements ColumnAccessor
{
    private final Class<?> clazz;

    private final boolean primitive;


    public BasicColumnAccessor(final Field field, final Class<?> clazz)
    {
        super(field);
        if (null == clazz)
        {
            throw new NullArgumentException("class");
        }
        this.clazz = clazz;
        if (this.clazz.equals(long.class) ||
            this.clazz.equals(int.class) ||
            this.clazz.equals(char.class) ||
            this.clazz.equals(short.class) ||
            this.clazz.equals(boolean.class))
        {
            this.primitive = true;
        }
        else
        {
            this.primitive = false;
        }
    }


    @Override
    public boolean isLoaded(final Object entity) throws NormandraException
    {
        return true;
    }


    @Override
    public boolean isEmpty(final Object entity) throws NormandraException
    {
        final Object obj = this.getValue(entity, null);
        if (null == obj)
        {
            return true;
        }
        if (obj instanceof Number)
        {
            return ((Number) obj).longValue() == 0;
        }
        else
        {
            return false;
        }
    }


    @Override
    public Object getValue(final Object entity, EntitySession session) throws NormandraException
    {
        try
        {
            final Object obj = this.get(entity);
            if (null == obj)
            {
                return null;
            }
            if (this.primitive)
            {
                return obj;
            }
            else
            {
                return this.clazz.cast(obj);
            }
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get property [" + this.getField().getName() + "] on entity [" + entity + "].", e);
        }
    }


    @Override
    public boolean setValue(final Object entity, final DataHolder data, final EntitySession session) throws NormandraException
    {
        final Object value = data != null && !data.isEmpty() ? data.get() : null;
        if (this.primitive && null == value)
        {
            return false;
        }
        try
        {
            return this.set(entity, value);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to set property [" + this.getField().getName() + "] on entity [" + entity + "].", e);
        }
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        BasicColumnAccessor that = (BasicColumnAccessor) o;

        if (clazz != null ? !clazz.equals(that.clazz) : that.clazz != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (clazz != null ? clazz.hashCode() : 0);
        return result;
    }
}
