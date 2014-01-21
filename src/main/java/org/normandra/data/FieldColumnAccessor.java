package org.normandra.data;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.NullArgumentException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

/**
 * a generic column data accessor
 * <p/>
 * User: bowen
 * Date: 1/15/14
 */
abstract public class FieldColumnAccessor<T> implements ColumnAccessor<T>
{
    private final Field field;

    private transient Boolean hasGetter = null;


    public FieldColumnAccessor(final Field field)
    {
        if (null == field)
        {
            throw new NullArgumentException("field");
        }
        this.field = field;
    }


    public Field getField()
    {
        return this.field;
    }


    protected final Object get(final Object entity) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException
    {
        if (null == entity)
        {
            return null;
        }
        if (this.field.isAccessible())
        {
            return this.field.get(entity);
        }
        else if (this.checkGetMethod(entity))
        {
            return BeanUtils.getSimpleProperty(entity, this.field.getName());
        }
        else
        {
            this.field.setAccessible(true);
            return this.field.get(entity);
        }
    }


    private boolean checkGetMethod(final Object entity) throws InvocationTargetException, IllegalAccessException
    {
        if (null == this.hasGetter)
        {
            try
            {
                BeanUtils.getProperty(entity, this.field.getName());
                this.hasGetter = true;
            }
            catch (final NoSuchMethodException e)
            {
                this.hasGetter = false;
            }
        }
        return this.hasGetter.booleanValue();
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldColumnAccessor that = (FieldColumnAccessor) o;

        if (field != null ? !field.equals(that.field) : that.field != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        return field != null ? field.hashCode() : 0;
    }
}
