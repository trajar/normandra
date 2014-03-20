package org.normandra.meta;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.data.ColumnAccessor;
import org.normandra.data.IdAccessor;
import org.normandra.data.NullIdAccessor;
import org.normandra.generator.IdGenerator;
import org.normandra.util.ArraySet;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * entity meta-data
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
public class EntityMeta implements Iterable<TableMeta>, Comparable<EntityMeta>
{
    private final String name;

    private final Set<TableMeta> tables = new TreeSet<>();

    private final Class<?> type;

    private final Map<ColumnMeta, ColumnAccessor> accessors = new TreeMap<>();

    private final Map<ColumnMeta, IdGenerator> generators = new TreeMap<>();

    private IdAccessor id = NullIdAccessor.getInstance();


    public EntityMeta(final String name, final Class<?> clazz)
    {
        if (null == name || name.isEmpty())
        {
            throw new IllegalArgumentException("Name cannot be empty/null.");
        }
        if (null == clazz)
        {
            throw new NullArgumentException("class");
        }
        this.name = name;
        this.type = clazz;
    }


    public DiscriminatorMeta getDiscriminator()
    {
        for (final TableMeta table : this.tables)
        {
            for (final ColumnMeta column : table)
            {
                if (column instanceof DiscriminatorMeta)
                {
                    return (DiscriminatorMeta) column;
                }
            }
        }
        return null;
    }


    public Collection<ColumnMeta> getPrimaryKeys()
    {
        final Set<ColumnMeta> columns = new ArraySet<>();
        for (final TableMeta table : this.tables)
        {
            columns.addAll(table.getPrimaryKeys());
        }
        return Collections.unmodifiableSet(columns);
    }


    public Collection<TableMeta> getTables()
    {
        return Collections.unmodifiableCollection(this.tables);
    }


    public Iterable<Map.Entry<ColumnMeta, ColumnAccessor>> getAccessors()
    {
        return Collections.unmodifiableMap(this.accessors).entrySet();
    }


    public ColumnAccessor getAccessor(final ColumnMeta<?> column)
    {
        if (null == column)
        {
            return null;
        }
        return this.accessors.get(column);
    }


    public ColumnAccessor getAccessor(final String columnName)
    {
        if (null == columnName || columnName.isEmpty())
        {
            return null;
        }
        for (final Map.Entry<ColumnMeta, ColumnAccessor> entry : this.accessors.entrySet())
        {
            final ColumnMeta column = entry.getKey();
            if (columnName.equalsIgnoreCase(column.getName()) || columnName.equalsIgnoreCase(column.getProperty()))
            {
                return entry.getValue();
            }
        }
        return null;
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


    public boolean setAccessor(final ColumnMeta column, final ColumnAccessor accessor)
    {
        if (null == column)
        {
            return false;
        }
        if (null == accessor)
        {
            return this.accessors.remove(column) != null;
        }
        else
        {
            this.accessors.put(column, accessor);
            return true;
        }
    }


    boolean putColumns(final TableMeta table, final Map<ColumnMeta, ColumnAccessor> map)
    {
        if (null == map || map.isEmpty())
        {
            return false;
        }
        if (!this.tables.contains(table))
        {
            return false;
        }
        for (final Map.Entry<ColumnMeta, ColumnAccessor> entry : map.entrySet())
        {
            final ColumnMeta column = entry.getKey();
            final ColumnAccessor accessor = entry.getValue();
            table.addColumn(column);
            if (accessor != null)
            {
                this.setAccessor(column, accessor);
            }
        }
        return true;
    }


    @Override
    public Iterator<TableMeta> iterator()
    {
        return Collections.unmodifiableCollection(this.tables).iterator();
    }


    public TableMeta getTable(final String tableName)
    {
        if (null == tableName || tableName.isEmpty())
        {
            return null;
        }
        for (final TableMeta table : this.tables)
        {
            if (tableName.equalsIgnoreCase(table.getName()))
            {
                return table;
            }
        }
        return null;
    }


    protected boolean addTable(final TableMeta tbl)
    {
        if (null == tbl)
        {
            return false;
        }
        return this.tables.add(tbl);
    }


    protected boolean removeTable(final TableMeta tbl)
    {
        if (null == tbl)
        {
            return false;
        }
        return this.tables.remove(tbl);
    }


    protected void setId(final IdAccessor id)
    {
        if (null == id)
        {
            throw new NullArgumentException("id accessor");
        }
        this.id = id;
    }


    public IdAccessor getId()
    {
        return this.id;
    }


    public String getName()
    {
        return this.name;
    }


    public Class<?> getType()
    {
        return this.type;
    }


    @Override
    public int compareTo(final EntityMeta meta)
    {
        if (null == meta)
        {
            return -1;
        }
        return this.name.compareToIgnoreCase(meta.name);
    }


    @Override
    public String toString()
    {
        return this.name + " (" + this.type.getSimpleName() + ")";
    }


}
