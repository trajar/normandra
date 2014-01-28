package org.normandra.data;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.NormandraException;

import java.lang.reflect.Field;

/**
 * a nested column accessor
 * <p/>
 * User: bowen
 * Date: 1/21/14
 */
public class NestedFieldColumnAccessor<T> extends FieldColumnAccessor<T>
{
    private final ColumnAccessor<T> delegate;


    public NestedFieldColumnAccessor(final Field field, final ColumnAccessor<T> delegate)
    {
        super(field);
        if (null == delegate)
        {
            throw new NullArgumentException("accessor");
        }
        this.delegate = delegate;
    }


    @Override
    public boolean isEmpty(final Object entity) throws NormandraException
    {
        return this.getValue(entity) != null;
    }


    @Override
    public T getValue(final Object entity) throws NormandraException
    {
        final Object base;
        try
        {
            base = this.get(entity);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get base value for nested property [" + this.getField().getName() + "].", e);
        }
        if (null == base)
        {
            return null;
        }
        return this.delegate.getValue(base);
    }


    @Override
    public boolean setValue(Object entity, T value) throws NormandraException
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        NestedFieldColumnAccessor that = (NestedFieldColumnAccessor) o;

        if (delegate != null ? !delegate.equals(that.delegate) : that.delegate != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (delegate != null ? delegate.hashCode() : 0);
        return result;
    }
}
