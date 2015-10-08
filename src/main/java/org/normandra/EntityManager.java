package org.normandra;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.EntityMetaLookup;
import org.normandra.meta.HierarchyEntityContext;
import org.normandra.meta.SingleEntityContext;

/**
 * an entity manager backed by NoSQL database
 * <p>
 * User: bowen
 * Date: 8/31/13
 */
public class EntityManager implements Transactional
{
    private final DatabaseSession database;

    private final EntityMetaLookup lookup;


    public EntityManager(final DatabaseSession db, final EntityMetaLookup lookup)
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


    @Override
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


    public <T> T get(final Class<? extends T> clazz, final Object key) throws NormandraException
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
        return (T) obj;
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


    @Override
    public void withTransaction(TransactionRunnable worker) throws NormandraException
    {
        this.database.withTransaction(worker);
    }


    @Override
    public Transaction beginTransaction() throws NormandraException
    {
        return this.database.beginTransaction();
    }


    @Override
    public boolean pendingWork()
    {
        return this.database.pendingWork();
    }


    @Override
    public void beginWork() throws NormandraException
    {
        this.database.beginWork();
    }


    @Override
    public void commitWork() throws NormandraException
    {
        this.database.commitWork();
    }


    @Override
    public void rollbackWork() throws NormandraException
    {
        this.database.rollbackWork();
    }
}
