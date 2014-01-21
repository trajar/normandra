package org.normandra.data;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.NormandraException;

import java.lang.reflect.Field;

/**
 * a generic column data accessor
 * <p/>
 * User: bowen
 * Date: 1/15/14
 */
public class BasicFieldColumnAccessor<T> extends FieldColumnAccessor<T>
{
    private final Class<T> clazz;

    private final boolean primitive;


    public BasicFieldColumnAccessor(final Field field, final Class<T> clazz)
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
            this.clazz.equals(short.class))
        {
            this.primitive = true;
        }
        else
        {
            this.primitive = false;
        }
    }


    @Override
    public T getValue(final Object entity) throws NormandraException
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
                return (T) obj;
            }
            else
            {
                return this.clazz.cast(obj);
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

        BasicFieldColumnAccessor that = (BasicFieldColumnAccessor) o;

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
