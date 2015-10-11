package org.normandra.data;

import org.apache.commons.lang.NullArgumentException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

/**
 * a generic column data accessor
 * <p/>
 * 
 * Date: 1/15/14
 */
abstract public class FieldColumnAccessor
{
    private final Field field;


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


    protected final boolean set(final Object entity, final Object value) throws IllegalAccessException, InvocationTargetException
    {
        if (null == entity)
        {
            return false;
        }
        if (!this.field.isAccessible())
        {
            this.field.setAccessible(true);
        }
        this.field.set(entity, value);
//      BeanUtil.setPropertyForced(entity, this.field.g);
        return true;
    }


    protected final Object get(final Object entity) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException
    {
        if (null == entity)
        {
            return null;
        }
        if (!this.field.isAccessible())
        {
            this.field.setAccessible(true);
        }
        return this.field.get(entity);
//      return BeanUtil.getProperty(entity, this.field.getName());
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
