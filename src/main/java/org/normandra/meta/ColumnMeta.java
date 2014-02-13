package org.normandra.meta;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.data.ColumnAccessor;

/**
 * column meta-data
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
public class ColumnMeta<T> implements Comparable<ColumnMeta>
{
    private final String name;

    private final String property;

    private final Class<T> type;

    private final ColumnAccessor accessor;

    private boolean primaryKey;


    public ColumnMeta(final String name, final String property, final ColumnAccessor accessor, final Class<T> clazz, final boolean primaryKey)
    {
        if (null == name)
        {
            throw new NullArgumentException("name");
        }
        if (null == property)
        {
            throw new NullArgumentException("property");
        }
        if (null == accessor)
        {
            throw new NullArgumentException("accessor");
        }
        if (null == clazz)
        {
            throw new NullArgumentException("class");
        }
        this.name = name;
        this.property = property;
        this.type = clazz;
        this.accessor = accessor;
        this.primaryKey = primaryKey;
    }


    public String getName()
    {
        return this.name;
    }


    public String getProperty()
    {
        return this.property;
    }


    public ColumnAccessor getAccessor()
    {
        return this.accessor;
    }


    public Class<T> getType()
    {
        return this.type;
    }


    public boolean isPrimaryKey()
    {
        return this.primaryKey;
    }


    @Override
    public int compareTo(final ColumnMeta meta)
    {
        if (null == meta)
        {
            return 1;
        }
        return this.name.compareToIgnoreCase(meta.name);
    }


    @Override
    public String toString()
    {
        return this.name + " (" + this.type.getSimpleName() + ")";
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnMeta that = (ColumnMeta) o;

        if (primaryKey != that.primaryKey) return false;
        if (accessor != null ? !accessor.equals(that.accessor) : that.accessor != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (property != null ? !property.equals(that.property) : that.property != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (property != null ? property.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (accessor != null ? accessor.hashCode() : 0);
        result = 31 * result + (primaryKey ? 1 : 0);
        return result;
    }
}
