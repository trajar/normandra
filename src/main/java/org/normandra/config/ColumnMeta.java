package org.normandra.config;

import org.apache.commons.lang.NullArgumentException;

/**
 * column meta-data
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
public class ColumnMeta implements Comparable<ColumnMeta>
{
    private final String name;

    private final Class<?> type;

    private boolean primaryKey;


    public ColumnMeta(final String name, final Class<?> clazz, final boolean primaryKey)
    {
        if (null == name)
        {
            throw new NullArgumentException("name");
        }
        if (null == clazz)
        {
            throw new NullArgumentException("class");
        }
        this.name = name;
        this.type = clazz;
        this.primaryKey = primaryKey;
    }


    public String getName()
    {
        return name;
    }


    public Class<?> getType()
    {
        return type;
    }


    public boolean isPrimaryKey()
    {
        return primaryKey;
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
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (primaryKey ? 1 : 0);
        return result;
    }
}
