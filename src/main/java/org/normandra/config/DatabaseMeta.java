package org.normandra.config;

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
    private Set<EntityMeta> entities = new TreeSet<>();


    public DatabaseMeta(final Collection<EntityMeta> c)
    {
        if (null == c || c.isEmpty())
        {
            throw new IllegalArgumentException("Entities cannot be null/empty.");
        }
        this.entities.addAll(c);
    }


    public Collection<EntityMeta> getEntities()
    {
        return Collections.unmodifiableCollection(this.entities);
    }


    @Override
    public Iterator<EntityMeta> iterator()
    {
        return Collections.unmodifiableCollection(this.entities).iterator();
    }
}
