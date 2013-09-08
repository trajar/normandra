package org.normandra.config;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * database meta-data
 * <p/>
 * User: bowen
 * Date: 9/4/13
 */
public class DatabaseMeta
{
    private Set<EntityMeta> entities = new TreeSet<>();


    public DatabaseMeta(final Collection<EntityMeta> c)
    {
        if (null == c || c.isEmpty())
        {
            throw new IllegalArgumentException("Entities cannot be null/empty.");
        }
        this.entities.addAll(c);
    }


    public Set<String> getTables()
    {
        final Set<String> list = new TreeSet<>();
        for (final EntityMeta meta : this.entities)
        {
            list.add(meta.getTable());
        }
        return Collections.unmodifiableSet(list);
    }


    public Map<String, Collection<EntityMeta>> getEntities()
    {
        final Map<String, Collection<EntityMeta>> map = new TreeMap<>();
        for (final EntityMeta meta : this.entities)
        {
            final String table = meta.getTable();
            Collection<EntityMeta> list = map.get(table);
            if (null == list)
            {
                list = new LinkedList<>();
                map.put(table, list);
            }
            list.add(meta);
        }
        return Collections.unmodifiableMap(map);
    }


    public Collection<EntityMeta> getEntities(final String table)
    {
        if (null == table || table.isEmpty())
        {
            return Collections.emptyList();
        }
        final List<EntityMeta> list = new LinkedList<>();
        for (final EntityMeta meta : this.entities)
        {
            if (table.equalsIgnoreCase(meta.getTable()))
            {
                list.add(meta);
            }
        }
        return Collections.unmodifiableCollection(list);
    }
}
