package org.normandra.meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * database meta-data
 * <p/>
 * User: bowen
 * Date: 9/4/13
 */
public class DatabaseMeta implements Iterable<EntityMeta>
{
    private final Collection<EntityMeta> entities;


    public DatabaseMeta(final Collection<EntityMeta> c)
    {
        if (null == c || c.isEmpty())
        {
            throw new IllegalArgumentException("Entities cannot be null/empty.");
        }
        this.entities = new ArrayList<>(c);
    }


    public Collection<String> getTables()
    {
        final Set<String> tables = new TreeSet<>();
        for (final EntityMeta meta : this.entities)
        {
            for (final TableMeta table : meta)
            {
                tables.add(table.getName());
            }
        }
        return Collections.unmodifiableCollection(tables);
    }


    public Collection<EntityMeta> getEntities()
    {
        return Collections.unmodifiableCollection(this.entities);
    }


    public EntityMeta getEntity(final String name)
    {
        if (null == name || name.isEmpty())
        {
            return null;
        }
        for (final EntityMeta meta : this.entities)
        {
            if (name.equalsIgnoreCase(meta.getName()))
            {
                return meta;
            }
            for (final TableMeta table : meta)
            {
                if (name.equalsIgnoreCase(table.getName()))
                {
                    return meta;
                }
            }
        }
        return null;
    }


    @Override
    public Iterator<EntityMeta> iterator()
    {
        return Collections.unmodifiableCollection(this.entities).iterator();
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DatabaseMeta that = (DatabaseMeta) o;

        if (entities != null ? !entities.equals(that.entities) : that.entities != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        return entities != null ? entities.hashCode() : 0;
    }
}
