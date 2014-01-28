package org.normandra.data;

import org.normandra.NormandraException;

/**
 * a constant read-only accessor
 * <p/>
 * User: bowen
 * Date: 1/15/14
 */
public class ReadOnlyColumnAccessor<T> implements ColumnAccessor<T>
{
    private final T value;


    public ReadOnlyColumnAccessor(final T value)
    {
        this.value = value;
    }


    @Override
    public boolean isEmpty(Object entity) throws NormandraException
    {
        return false;
    }


    @Override
    public T getValue(Object entity)
    {
        return this.value;
    }


    @Override
    public boolean setValue(Object entity, T value) throws NormandraException
    {
        return false;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReadOnlyColumnAccessor that = (ReadOnlyColumnAccessor) o;

        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        return value != null ? value.hashCode() : 0;
    }
}
