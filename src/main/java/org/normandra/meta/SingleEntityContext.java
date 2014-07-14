package org.normandra.meta;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.data.ColumnAccessor;
import org.normandra.data.IdAccessor;
import org.normandra.util.ArraySet;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * a single-entity meta context
 * <p>
 * User: bowen
 * Date: 3/30/14
 */
public class SingleEntityContext implements EntityContext
{
    private final EntityMeta entity;


    public SingleEntityContext(final EntityMeta meta)
    {
        if (null == meta)
        {
            throw new NullArgumentException("entity meta");
        }
        this.entity = meta;
    }


    @Override
    public IdAccessor getId()
    {
        return this.entity.getId();
    }


    @Override
    public Collection<EntityMeta> getEntities()
    {
        return Arrays.asList(this.entity);
    }


    @Override
    public Set<ColumnMeta> getPrimaryKeys()
    {
        final Set<ColumnMeta> columns = new ArraySet<>();
        for (final TableMeta table : this.entity)
        {
            if (!table.isJoinTable())
            {
                columns.addAll(table.getPrimaryKeys());
            }
        }
        return Collections.unmodifiableSet(columns);
    }


    @Override
    public Set<ColumnMeta> getColumns()
    {
        // query columns for which we have accessors
        final Set<ColumnMeta> columns = new ArraySet<>();
        for (final Map.Entry<ColumnMeta, ColumnAccessor> entry : this.entity.getAccessors())
        {
            if (entry.getValue() != null)
            {
                columns.add(entry.getKey());
            }
        }
        return Collections.unmodifiableSet(columns);
    }


    @Override
    public EntityMeta findEntity(final Map<ColumnMeta, Object> data)
    {
        if (null == data || data.isEmpty())
        {
            return null;
        }
        return this.entity;
    }


    @Override
    public TableMeta findTable(final String name)
    {
        for (final TableMeta table : this.entity)
        {
            if (table.getName().equalsIgnoreCase(name))
            {
                return table;
            }
        }
        return null;
    }


    @Override
    public Set<TableMeta> getTables()
    {
        return this.entity.getTables();
    }


    @Override
    public Set<TableMeta> getPrimaryTables()
    {
        final Collection<TableMeta> tables = this.entity.getTables();
        final Set<TableMeta> list = new ArraySet<>(tables.size());
        for (final TableMeta table : tables)
        {
            if (!table.isJoinTable())
            {
                list.add(table);
            }
        }
        return Collections.unmodifiableSet(list);
    }


    @Override
    public Set<TableMeta> getSecondaryTables()
    {
        final Collection<TableMeta> tables = this.entity.getTables();
        final Set<TableMeta> list = new ArraySet<>(tables.size());
        for (final TableMeta table : tables)
        {
            if (table.isJoinTable())
            {
                list.add(table);
            }
        }
        return Collections.unmodifiableSet(list);
    }


    @Override
    public ColumnMeta getPrimaryKey()
    {
        for (final TableMeta table : this.entity)
        {
            if (!table.isJoinTable())
            {
                for (final ColumnMeta column : table)
                {
                    if (column.isPrimaryKey())
                    {
                        return column;
                    }
                }
            }
        }
        return null;
    }


    @Override
    public String toString()
    {
        return this.entity.toString();
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        SingleEntityContext that = (SingleEntityContext) o;

        if (entity != null ? !entity.equals(that.entity) : that.entity != null)
        {
            return false;
        }

        return true;
    }


    @Override
    public int hashCode()
    {
        return entity != null ? entity.hashCode() : 0;
    }
}
