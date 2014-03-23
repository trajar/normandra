package org.normandra.data;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.DatabaseSession;
import org.normandra.NormandraException;

import java.lang.reflect.Field;

/**
 * a nested column accessor
 * <p>
 * User: bowen
 * Date: 1/21/14
 */
public class NestedColumnAccessor extends FieldColumnAccessor implements ColumnAccessor
{
    private final ColumnAccessor delegate;

    private final Class<?> type;


    public NestedColumnAccessor(final Field field, final ColumnAccessor delegate)
    {
        super(field);
        if (null == delegate)
        {
            throw new NullArgumentException("accessor");
        }
        this.type = field.getType();
        this.delegate = delegate;
    }


    @Override
    public boolean isLoaded(final Object entity) throws NormandraException
    {
        return true;
    }


    @Override
    public boolean isEmpty(final Object entity) throws NormandraException
    {
        return this.getValue(entity) != null;
    }


    @Override
    public Object getValue(final Object entity) throws NormandraException
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
    public boolean setValue(final Object entity, final Object value, final DatabaseSession session) throws NormandraException
    {
        Object base;
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
            if (null == value)
            {
                return false;
            }
            try
            {
                base = this.type.newInstance();
                this.set(entity, base);
            }
            catch (final Exception e)
            {
                throw new NormandraException("Unable to instantiate new instance of nested/embedded type [" + this.type + "] for property [" + this.getField().getName() + "].", e);
            }
        }

        if (null == base)
        {
            return false;
        }
        return this.delegate.setValue(base, value, session);
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        NestedColumnAccessor that = (NestedColumnAccessor) o;

        if (delegate != null ? !delegate.equals(that.delegate) : that.delegate != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (delegate != null ? delegate.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }
}
