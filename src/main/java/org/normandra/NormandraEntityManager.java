package org.normandra;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.functors.InstanceofPredicate;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.DiscriminatorMeta;
import org.normandra.meta.EntityMeta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * an entity manager backed by NoSQL database
 * <p/>
 * User: bowen
 * Date: 8/31/13
 */
public class NormandraEntityManager
{
    private final NormandraDatabaseSession database;

    private final DatabaseMeta meta;

    private final Map<Class<?>, EntityMeta<?>> classMap;


    protected NormandraEntityManager(final NormandraDatabaseSession db, final DatabaseMeta meta)
    {
        if (null == db)
        {
            throw new NullArgumentException("database");
        }
        if (null == meta)
        {
            throw new NullArgumentException("metadata");
        }
        this.database = db;
        this.meta = meta;

        final int size = meta.getEntities().size();
        this.classMap = new HashMap<>(size);
        for (final EntityMeta entity : this.meta)
        {
            this.classMap.put(entity.getType(), entity);
        }
    }


    public void close()
    {
        this.database.close();
    }


    public <T> boolean exists(final Class<T> clazz, final Object key) throws NormandraException
    {
        if (null == clazz)
        {
            throw new NullArgumentException("element");
        }
        if (null == key)
        {
            return false;
        }

        final List<EntityMeta> list = this.findMeta(clazz);
        if (list.isEmpty())
        {
            return false;
        }

        final EntityMeta meta = list.get(0);
        return this.database.exists(meta, key);
    }


    public <T> T get(final Class<T> clazz, final Object key) throws NormandraException
    {
        if (null == clazz)
        {
            throw new NullArgumentException("element");
        }
        if (null == key)
        {
            return null;
        }

        final List<EntityMeta> list = this.findMeta(clazz);
        if (list.size() == 1)
        {
            // simple entity
            final EntityMeta meta = list.get(0);
            return (T) this.database.get(meta, key);
        }
        else
        {
            // inherited entity
            final Object typeDescriminator = this.database.discriminator(list.get(0), key);
            if (null == typeDescriminator)
            {
                return null;
            }
            for (final EntityMeta meta : list)
            {
                final DiscriminatorMeta descrim = (DiscriminatorMeta) CollectionUtils.find(meta.getColumns(), InstanceofPredicate.getInstance(DiscriminatorMeta.class));
                final Object type = descrim.getValue();
                if (typeDescriminator.equals(type))
                {
                    return (T) this.database.get(meta, key);
                }
            }
            return null;
        }
    }


    public <T> void save(final T element) throws NormandraException
    {
        if (null == element)
        {
            throw new NullArgumentException("element");
        }

        final Class<?> clazz = element.getClass();
        final EntityMeta meta = this.classMap.get(clazz);
        if (null == meta)
        {
            throw new IllegalArgumentException("Element [" + element + "] is not a registered entity.");
        }

        this.database.save(meta, element);
    }


    private List<EntityMeta> findMeta(final Class<?> clazz)
    {
        final EntityMeta existing = this.classMap.get(clazz);
        if (existing != null)
        {
            return Arrays.asList(existing);
        }
        final List<EntityMeta> list = new ArrayList<>();
        for (final Map.Entry<Class<?>, EntityMeta<?>> entry : this.classMap.entrySet())
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
}
