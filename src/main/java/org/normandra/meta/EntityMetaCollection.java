/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.normandra.meta;

import org.apache.commons.lang.NullArgumentException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * a set of entity meta instances
 *
 * 
 */
public class EntityMetaCollection implements EntityMetaLookup, Iterable<EntityMeta>
{
    private final Map<Class, EntityMeta> classMap = new HashMap<>();

    public EntityMetaCollection(final Iterable<EntityMeta> metas)
    {
        if (null == metas)
        {
            throw new NullArgumentException("metadata");
        }
        for (final EntityMeta entity : metas)
        {
            this.classMap.put(entity.getType(), entity);
        }
    }

    @Override
    public EntityMeta getMeta(final Class<?> clazz)
    {
        if (null == clazz)
        {
            return null;
        }
        final EntityMeta existing = this.classMap.get(clazz);
        if (existing != null)
        {
            return existing;
        }
        final List<EntityMeta> list = this.findMeta(clazz);
        if (list.isEmpty())
        {
            return null;
        }
        if (list.size() != 1)
        {
            throw new IllegalArgumentException("Found multiple entities for [" + clazz + "].");
        }
        return list.get(0);
    }

    @Override
    public final EntityMeta getMeta(final String labelOrType)
    {
        if (null == labelOrType || labelOrType.isEmpty())
        {
            return null;
        }
        for (final EntityMeta meta : this.classMap.values())
        {
            if (labelOrType.equalsIgnoreCase(meta.getName()))
            {
                return meta;
            }
            for (final TableMeta table : meta)
            {
                if (!table.isJoinTable() && labelOrType.equalsIgnoreCase(table.getName()))
                {
                    return meta;
                }
            }
        }
        return null;
    }

    @Override
    public final List<EntityMeta> findMeta(final Class<?> clazz)
    {
        if (null == clazz)
        {
            return Collections.emptyList();
        }
        final EntityMeta existing = this.classMap.get(clazz);
        if (existing != null)
        {
            return Arrays.asList(existing);
        }
        final List<EntityMeta> list = new ArrayList<>(4);
        for (final Map.Entry<Class, EntityMeta> entry : this.classMap.entrySet())
        {
            final Class<?> entityClass = entry.getKey();
            final EntityMeta entityMeta = entry.getValue();
            if (entityClass.isAssignableFrom(clazz))
            {
                list.add(entityMeta);
            }
        }
        if (list.isEmpty())
        {
            return Collections.emptyList();
        }
        else
        {
            return Collections.unmodifiableList(list);
        }
    }

    @Override
    public final EntityContext findContext(final Class<?> clazz)
    {
        final List<EntityMeta> list = this.findMeta(clazz);
        if (list.isEmpty())
        {
            return null;
        }
        if (list.size() == 1)
        {
            // simple entity
            final EntityMeta meta = list.get(0);
            return new SingleEntityContext(meta);
        }
        else
        {
            // inherited entity
            return new HierarchyEntityContext(list);
        }
    }

    @Override
    public Collection<EntityMeta> list()
    {
        return Collections.unmodifiableCollection(this.classMap.values());
    }

    @Override
    public boolean contains(final Class<?> clazz)
    {
        return !this.findMeta(clazz).isEmpty();
    }

    @Override
    public boolean contains(final EntityMeta meta)
    {
        if (null == meta)
        {
            return false;
        }
        for (final Map.Entry<Class, EntityMeta> entry : this.classMap.entrySet())
        {
            if (meta == entry.getValue() || meta.compareTo(entry.getValue()) == 0)
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public int size()
    {
        return this.classMap.size();
    }

    @Override
    public Iterator<EntityMeta> iterator()
    {
        return Collections.unmodifiableCollection(this.classMap.values()).iterator();
    }
}
