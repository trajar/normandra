package org.normandra.meta;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.data.ColumnAccessor;
import org.normandra.data.IdAccessor;
import org.normandra.data.NullIdAccessor;
import org.normandra.generator.IdGenerator;
import org.normandra.generator.UUIDGenerator;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

/**
 * entity meta-data
 * <p>
 * 
 * Date: 9/1/13
 */
public class EntityMeta implements Iterable<TableMeta>, Comparable<EntityMeta>
{
    private final String name;

    private final Set<TableMeta> tables = new TreeSet<>();

    private final Class<?> type;

    private String inherited;

    private DiscriminatorMeta discriminator;

    private final Set<ColumnMeta> indexed = new TreeSet<>();

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
        return this.discriminator;
    }


    public Set<TableMeta> getTables()
    {
        return Collections.unmodifiableSet(this.tables);
    }


    public String getInherited()
    {
        return this.inherited;
    }


    public void setInherited(final String inherited)
    {
        this.inherited = inherited;
    }


    public boolean addIndexed(final ColumnMeta column)
    {
        if (null == column)
        {
            return false;
        }
        return this.indexed.add(column);
    }


    public boolean removeIndexed(final ColumnMeta column)
    {
        if (null == column)
        {
            return false;
        }
        return this.indexed.remove(column);
    }


    public boolean isIndexed(final ColumnMeta column)
    {
        if (null == column)
        {
            return false;
        }
        return this.indexed.contains(column);
    }


    public Set<ColumnMeta> getIndexed()
    {
        return Collections.unmodifiableSet(this.indexed);
    }


    public Iterable<Map.Entry<ColumnMeta, ColumnAccessor>> getAccessors()
    {
        return Collections.unmodifiableMap(this.accessors).entrySet();
    }


    public ColumnAccessor getAccessor(final ColumnMeta column)
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


    protected void setDiscriminator(DiscriminatorMeta discriminator)
    {
        this.discriminator = discriminator;
    }


    public IdGenerator getGenerator(final ColumnMeta column)
    {
        if (null == column)
        {
            return null;
        }
        return this.generators.get(column);
    }


    public boolean setGenerator(final ColumnMeta column, final IdGenerator generator)
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
            if (column.isPrimaryKey() && column.getType().equals(UUID.class))
            {
                this.setGenerator(column, UUIDGenerator.getInstance());
            }
            this.setAccessor(column, accessor);
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
            return 1;
        }
        return this.name.compareToIgnoreCase(meta.name);
    }


    @Override
    public String toString()
    {
        return this.name;
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

        EntityMeta that = (EntityMeta) o;

        if (discriminator != null ? !discriminator.equals(that.discriminator) : that.discriminator != null)
        {
            return false;
        }
        if (id != null ? !id.equals(that.id) : that.id != null)
        {
            return false;
        }
        if (inherited != null ? !inherited.equals(that.inherited) : that.inherited != null)
        {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null)
        {
            return false;
        }
        if (tables != null ? !tables.equals(that.tables) : that.tables != null)
        {
            return false;
        }
        if (type != null ? !type.equals(that.type) : that.type != null)
        {
            return false;
        }

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (tables != null ? tables.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (inherited != null ? inherited.hashCode() : 0);
        result = 31 * result + (discriminator != null ? discriminator.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }
}
