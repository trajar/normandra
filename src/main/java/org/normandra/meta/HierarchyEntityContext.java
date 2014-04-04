package org.normandra.meta;

import org.normandra.data.ColumnAccessor;
import org.normandra.data.IdAccessor;
import org.normandra.util.ArraySet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * a complex entity hierarchy
 * <p>
 * User: bowen
 * Date: 3/30/14
 */
public class HierarchyEntityContext implements EntityContext
{
    private final List<EntityMeta> entities;


    public HierarchyEntityContext(final Collection<EntityMeta> c)
    {
        if (null == c || c.isEmpty())
        {
            throw new IllegalArgumentException("Entity collection cannot be null/empty.");
        }
        this.entities = new ArrayList<>(c);
    }


    @Override
    public IdAccessor getId()
    {
        return this.entities.get(0).getId();
    }


    @Override
    public Collection<EntityMeta> getEntities()
    {
        return Collections.unmodifiableCollection(this.entities);
    }


    @Override
    public Set<TableMeta> getTables()
    {
        final Set<TableMeta> tables = new ArraySet<>();
        for (final EntityMeta entity : this.entities)
        {
            tables.addAll(entity.getTables());
        }
        return Collections.unmodifiableSet(tables);
    }


    @Override
    public Set<TableMeta> getPrimaryTables()
    {
        final Set<TableMeta> tables = new ArraySet<>();
        for (final TableMeta table : this.getTables())
        {
            if (!table.isSecondary())
            {
                tables.add(table);
            }
        }
        return Collections.unmodifiableSet(tables);
    }


    @Override
    public Set<TableMeta> getSecondaryTables()
    {
        final Set<TableMeta> tables = new ArraySet<>();
        for (final TableMeta table : this.getTables())
        {
            if (table.isSecondary())
            {
                tables.add(table);
            }
        }
        return Collections.unmodifiableSet(tables);
    }


    @Override
    public Set<ColumnMeta> getPrimaryKeys()
    {
        final Set<ColumnMeta> columnns = new ArraySet<>();
        for (final TableMeta table : this.getTables())
        {
            for (final ColumnMeta column : table)
            {
                if (column.isPrimaryKey())
                {
                    columnns.add(column);
                }
            }
        }
        return Collections.unmodifiableSet(columnns);
    }


    @Override
    public Set<ColumnMeta> getColumns()
    {
        // query columns for which we have accessors
        final Set<ColumnMeta> columns = new ArraySet<>();
        for (final EntityMeta entity : this.entities)
        {
            for (final Map.Entry<ColumnMeta, ColumnAccessor> entry : entity.getAccessors())
            {
                if (entry.getValue() != null)
                {
                    columns.add(entry.getKey());
                }
            }
        }
        return Collections.unmodifiableSet(columns);
    }


    @Override
    public EntityMeta findEntity(final Map<ColumnMeta, Object> data)
    {
        // lookup entity based on discriminator value
        if (null == data || data.isEmpty())
        {
            return null;
        }
        final DiscriminatorMeta discrim = this.entities.get(0).getDiscriminator();
        if (null == discrim)
        {
            return null;
        }
        final Object discrimValue = data.get(discrim.getColumn());
        if (null == discrimValue)
        {
            return null;
        }
        for (final EntityMeta entity : this.entities)
        {
            final DiscriminatorMeta meta = entity.getDiscriminator();
            if (meta != null)
            {
                if (discrimValue.equals(meta.getValue()))
                {
                    return entity;
                }
            }
        }
        return null;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HierarchyEntityContext that = (HierarchyEntityContext) o;

        if (entities != null ? !entities.equals(that.entities) : that.entities != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        return entities != null ? entities.hashCode() : 0;
    }
}
