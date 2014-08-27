package org.normandra;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.HierarchyEntityContext;
import org.normandra.meta.SingleEntityContext;

import java.util.Collections;
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

    private final EntityLookup lookup;


    protected EntityManager(final DatabaseSession db, final EntityLookup lookup)
    {
        if (null == db)
        {
            throw new NullArgumentException("database");
        }
        if (null == lookup)
        {
            throw new NullArgumentException("lookup");
        }
        this.database = db;
        this.lookup = lookup;
    }


    public void close()
    {
        this.database.close();
    }


    public void clear() throws NormandraException
    {
        this.database.clear();
    }


    public <T> DatabaseQuery<T> query(final Class<T> clazz, final String name) throws NormandraException
    {
        return this.query(clazz, name, Collections.emptyMap());
    }


    public <T> DatabaseQuery<T> query(final Class<T> clazz, final String name, final Map<String, Object> parameters) throws NormandraException
    {
        if (null == clazz)
        {
            throw new NullArgumentException("type");
        }

        final List<EntityMeta> list = this.lookup.findMeta(clazz);
        if (list.isEmpty())
        {
            return null;
        }

        if (list.size() == 1)
        {
            return this.database.executeNamedQuery(new SingleEntityContext(list.get(0)), name, parameters);
        }
        else
        {
            return this.database.executeNamedQuery(new HierarchyEntityContext(list), name, parameters);
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

        final List<EntityMeta> list = this.lookup.findMeta(clazz);
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

        final EntityContext context = this.lookup.findContext(clazz);
        if (null == context)
        {
            return null;
        }
        final Object obj = this.database.get(context, key);
        if (null == obj)
        {
            return null;
        }
        return clazz.cast(obj);
    }


    public <T> void delete(final T element) throws NormandraException
    {
        if (null == element)
        {
            throw new NullArgumentException("element");
        }

        final Class<?> clazz = element.getClass();
        final EntityMeta meta = this.lookup.getMeta(clazz);
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
        final EntityMeta meta = this.lookup.getMeta(clazz);
        if (null == meta)
        {
            throw new IllegalArgumentException("Element [" + element + "] is not a registered entity.");
        }

        this.database.save(meta, element);
    }


    /**
     * returns true if this session has begun a unit of work (i.e. transaction)
     */
    public boolean pendingWork()
    {
        return this.database.pendingWork();
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
}
