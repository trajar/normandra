package org.normandra;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.meta.DatabaseMeta;
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
 * an entity manager backed by NoSQL database
 * <p>
 * User: bowen
 * Date: 8/31/13
 */
public class EntityManager
{
    private final DatabaseSession database;

    private final DatabaseMeta meta;

    private final Map<Class, EntityMeta> classMap;


    protected EntityManager(final DatabaseSession db, final DatabaseMeta meta)
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


    public void clear() throws NormandraException
    {
        this.database.clear();
    }


    public <T> DatabaseQuery<T> query(final Class<T> clazz, final String query, final Map<String, Object> parameters) throws NormandraException
    {
        if (null == clazz)
        {
            throw new NullArgumentException("element");
        }

        final List<EntityMeta> list = this.findMeta(clazz);
        if (list.isEmpty())
        {
            return null;
        }

        if (list.size() == 1)
        {
            return this.database.query(new SingleEntityContext(list.get(0)), query, parameters);
        }
        else
        {
            return this.database.query(new HierarchyEntityContext(list), query, parameters);
        }
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
        if (null == meta)
        {
            return false;
        }
        return this.database.exists(new SingleEntityContext(meta), key);
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
            final Object obj = this.database.get(new SingleEntityContext(meta), key);
            if (null == obj)
            {
                return null;
            }
            return clazz.cast(obj);
        }
        else
        {
            // inherited entity
            final Object obj = this.database.get(new HierarchyEntityContext(list), key);
            if (null == obj)
            {
                return null;
            }
            return clazz.cast(obj);
        }
    }


    public <T> void delete(final T element) throws NormandraException
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

        this.database.delete(meta, element);
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


    /**
     * being unit of work
     */
    public void beginWork() throws NormandraException
    {
        this.database.beginWork();
    }


    /**
     * commit unit of work, executing any stored/batched operations
     */
    public void commitWork() throws NormandraException
    {
        this.database.commitWork();
    }


    /**
     * rollback unit of work, clearing stored/batched operations
     */
    public void rollbackWork() throws NormandraException
    {
        this.database.rollbackWork();
    }


    private List<EntityMeta> findMeta(final Class<?> clazz)
    {
        final EntityMeta existing = this.classMap.get(clazz);
        if (existing != null)
        {
            return Arrays.asList(existing);
        }
        final List<EntityMeta> list = new ArrayList<>();
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
}
