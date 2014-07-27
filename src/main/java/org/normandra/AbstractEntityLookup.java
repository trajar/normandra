package org.normandra;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.HierarchyEntityContext;
import org.normandra.meta.SingleEntityContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * an abstract entity lookup class
 * <p>
 * User: bowen
 * Date: 7/23/14
 */
abstract public class AbstractEntityLookup implements EntityLookup
{
    private final Map<Class, EntityMeta> classMap = new HashMap<>();


    public AbstractEntityLookup(final Iterable<EntityMeta> metas)
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
        if (list.size() == 1)
        {
            return list.get(0);
        }
        else
        {
            return null;
        }
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
        }
        return null;
    }


    @Override
    public final List<EntityMeta> findMeta(final Class<?> clazz)
    {
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
            if (clazz.isAssignableFrom(entityClass))
            {
                list.add(entityMeta);
            }
        }
        return Collections.unmodifiableList(list);
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
}
