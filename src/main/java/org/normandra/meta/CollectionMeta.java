package org.normandra.meta;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.data.ColumnAccessor;

import java.util.Collection;

/**
 * collection column meta-data
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
public class CollectionMeta<T extends Collection> extends ColumnMeta<T>
{
    private final Class<?> generic;


    public CollectionMeta(final String name, final String property, final ColumnAccessor accessor, final Class<T> clazz, final Class<?> generic)
    {
        super(name, property, accessor, clazz, false);
        if (null == generic)
        {
            throw new NullArgumentException("generic");
        }
        this.generic = generic;
    }


    public Class<?> getGeneric()
    {
        return this.generic;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        CollectionMeta that = (CollectionMeta) o;

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
