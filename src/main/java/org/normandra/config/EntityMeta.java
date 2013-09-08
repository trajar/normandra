package org.normandra.config;

import org.apache.commons.lang.NullArgumentException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * entity meta-data
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
public class EntityMeta implements Iterable<ColumnMeta>, Comparable<EntityMeta>
{
    private final String name;

    private final String table;

    private final Class<?> type;

    private final List<ColumnMeta> columns;


    public EntityMeta(final String name, final String table, final Class<?> clazz, final Collection<ColumnMeta> c)
    {
        if (null == name || name.isEmpty())
        {
            throw new IllegalArgumentException("Name cannot be empty/null.");
        }
        if (null == table || table.isEmpty())
        {
            throw new IllegalArgumentException("Table cannot be empty/null.");
        }
        if (null == clazz)
        {
            throw new NullArgumentException("class");
        }
        if (null == c || c.isEmpty())
        {
            throw new IllegalArgumentException("Columns cannot be empty/null.");
        }
        this.name = name;
        this.table = table;
        this.type = clazz;
        this.columns = new ArrayList<>(c);
    }


    public String getName()
    {
        return name;
    }


    public String getTable()
    {
        return table;
    }


    public Class<?> getType()
    {
        return type;
    }


    public boolean hasColumn(final String name)
    {
        if (null == name || name.isEmpty())
        {
            return false;
        }
        for (final ColumnMeta meta : this)
        {
            if (name.equalsIgnoreCase(meta.getName()))
            {
                return true;
            }
        }
        return false;
    }


    public Collection<ColumnMeta> getPrimaryColumns()
    {
        final List<ColumnMeta> list = new LinkedList<>();
        for (final ColumnMeta column : this.columns)
        {
            if (column.isPrimaryKey())
            {
                list.add(column);
            }
        }
        return Collections.unmodifiableCollection(list);
    }


    @Override
    public Iterator<ColumnMeta> iterator()
    {
        return Collections.unmodifiableCollection(this.columns).iterator();
    }


    @Override
    public int compareTo(final EntityMeta meta)
    {
        if (null == meta)
        {
            return 1;
        }
        return this.name.compareToIgnoreCase(meta.name);
    }
}
