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
}
