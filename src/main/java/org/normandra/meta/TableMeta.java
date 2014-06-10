package org.normandra.meta;

import org.normandra.util.ArraySet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * a generic table meta
 * <p/>
 * User: bowen
 * Date: 3/14/14
 */
public class TableMeta implements Iterable<ColumnMeta>, Comparable<TableMeta>
{
    private final boolean joinTable;

    private final String name;

    private final Collection<ColumnMeta> columns = new ArraySet<>();


    public TableMeta(final String table, final boolean join)
    {
        if (null == table || table.isEmpty())
        {
            throw new IllegalArgumentException("Table cannot be empty/null.");
        }
        this.name = table;
        this.joinTable = join;
    }


    public boolean isJoinTable()
    {
        return this.joinTable;
    }


    public String getName()
    {
        return this.name;
    }


    public Collection<ColumnMeta> getLazyLoaded()
    {
        final List<ColumnMeta> list = new ArrayList<>();
        for (final ColumnMeta column : this.getColumns())
        {
            if (column.isLazyLoaded())
            {
                list.add(column);
            }
        }
        return Collections.unmodifiableList(list);
    }


    public Collection<ColumnMeta> getEagerLoaded()
    {
        final List<ColumnMeta> list = new ArrayList<>();
        for (final ColumnMeta column : this.getColumns())
        {
            if (!column.isLazyLoaded())
            {
                list.add(column);
            }
        }
        return Collections.unmodifiableList(list);
    }


    public Collection<ColumnMeta> getPrimaryKeys()
    {
        final List<ColumnMeta> keys = new ArrayList<>(4);
        for (final ColumnMeta column : this.getColumns())
        {
            if (column.isPrimaryKey())
            {
                keys.add(column);
            }
        }
        return Collections.unmodifiableList(keys);
    }


    public boolean hasColumn(final String nameOrProperty)
    {
        return this.getColumn(nameOrProperty) != null;
    }


    public Collection<ColumnMeta> getColumns()
    {
        return Collections.unmodifiableCollection(this.columns);
    }


    public ColumnMeta getColumn(final String nameOrProperty)
    {
        if (null == nameOrProperty || nameOrProperty.isEmpty())
        {
            return null;
        }
        for (final ColumnMeta meta : this.columns)
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


    boolean addColumn(final ColumnMeta column)
    {
        if (null == column)
        {
            return false;
        }
        return this.columns.add(column);
    }


    boolean removeColumn(final ColumnMeta column)
    {
        if (null == column)
        {
            return false;
        }
        return this.columns.remove(column);
    }


    @Override
    public Iterator<ColumnMeta> iterator()
    {
        return Collections.unmodifiableCollection(this.columns).iterator();
    }


    @Override
    public int compareTo(final TableMeta tbl)
    {
        if (null == tbl)
        {
            return 1;
        }
        return this.name.compareToIgnoreCase(tbl.name);
    }


    @Override
    public String toString()
    {
        return this.name + " (" + (this.joinTable ? "secondary" : "primary") + ")";
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableMeta that = (TableMeta) o;

        if (joinTable != that.joinTable) return false;
        if (columns != null ? !columns.equals(that.columns) : that.columns != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = (joinTable ? 1 : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (columns != null ? columns.hashCode() : 0);
        return result;
    }
}
