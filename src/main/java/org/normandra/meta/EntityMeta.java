package org.normandra.meta;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.generator.IdGenerator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * entity meta-data
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
public class EntityMeta<T> implements Iterable<ColumnMeta>, Comparable<EntityMeta>
{
    private final String name;

    private final String table;

    private final Class<T> type;

    private final List<ColumnMeta> columns;

    private final Map<ColumnMeta, IdGenerator> generators = new TreeMap<>();


    public EntityMeta(final String name, final String table, final Class<T> clazz, final Collection<ColumnMeta> c)
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


    public IdGenerator<?> getGenerator(final ColumnMeta column)
    {
        if (null == column)
        {
            return null;
        }
        return this.generators.get(column);
    }


    public boolean setGenerator(final ColumnMeta column, final IdGenerator<?> generator)
    {
        if (null == column)
        {
            return false;
        }
        if (null == generator)
        {
            return this.generators.remove(column) != null;
        }
        this.generators.put(column, generator);
        return true;
    }


    public String getName()
    {
        return name;
    }


    public String getTable()
    {
        return table;
    }


    public Class<T> getType()
    {
        return type;
    }


    public boolean hasColumn(final String nameOrProperty)
    {
        return this.getColumn(nameOrProperty) != null;
    }


    public Collection<ColumnMeta> getColumns()
    {
        return Collections.unmodifiableList(this.columns);
    }


    public ColumnMeta getColumn(final String nameOrProperty)
    {
        if (null == nameOrProperty || nameOrProperty.isEmpty())
        {
            return null;
        }
        for (final ColumnMeta meta : this)
        {
            if (nameOrProperty.equalsIgnoreCase(meta.getName()))
            {
                return meta;
            }
            if (nameOrProperty.equalsIgnoreCase(meta.getProperty()))
            {
                return meta;
            }
        }
        return null;
    }


    public ColumnMeta getPartition()
    {
        if (this.columns.isEmpty())
        {
            return null;
        }
        return this.getPrimary().iterator().next();
    }


    public Collection<ColumnMeta> getPrimary()
    {
        final List<ColumnMeta> list = new ArrayList<>(4);
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

        EntityMeta that = (EntityMeta) o;

        if (columns != null ? !columns.equals(that.columns) : that.columns != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (table != null ? !table.equals(that.table) : that.table != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (columns != null ? columns.hashCode() : 0);
        return result;
    }
}
