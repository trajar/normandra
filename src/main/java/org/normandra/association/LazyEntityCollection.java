package org.normandra.association;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.EntitySession;
import org.normandra.data.DataHolder;
import org.normandra.meta.EntityContext;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a lazy loaded entity collection
 * <p/>
 * User: bowen
 * Date: 3/25/14
 */
abstract public class LazyEntityCollection<T> implements LazyLoadedCollection<T>
{
    protected final DataHolder data;

    protected final EntityContext entity;

    protected final EntitySession session;

    private final CollectionFactory<T> factory;

    private Object[] keys;

    private Collection<T> entities;

    private final Object synch = new Object();

    private final AtomicBoolean loaded = new AtomicBoolean(false);


    public LazyEntityCollection(final EntitySession session, final EntityContext meta, final DataHolder data, final CollectionFactory<T> factory)
    {
        if (null == data)
        {
            throw new NullArgumentException("data holder");
        }
        if (null == session)
        {
            throw new NullArgumentException("session");
        }
        if (null == meta)
        {
            throw new NullArgumentException("entity");
        }
        if (null == factory)
        {
            throw new NullArgumentException("collection factory");
        }
        this.data = data;
        this.session = session;
        this.entity = meta;
        this.factory = factory;
    }


    @Override
    public boolean isLoaded()
    {
        return this.loaded.get();
    }


    protected Collection<T> getCollection()
    {
        return this.ensureEntities();
    }


    private Collection<T> ensureEntities()
    {
        if (this.entities != null)
        {
            return this.entities;
        }

        final Object[] keys = this.ensureKeys();

        synchronized (this.synch)
        {
            if (null == keys || keys.length <= 0)
            {
                this.entities = this.factory.create(0);
                return this.entities;
            }
            try
            {
                final List results = this.session.get(this.entity, keys);
                this.entities = this.factory.create(results.size());
                this.entities.addAll(results);
                return this.entities;
            }
            catch (final Exception e)
            {
                throw new IllegalStateException("Unable to query lazy entity collection with keys [" + keys + "].", e);
            }
        }
    }


    private Object[] ensureKeys()
    {
        // check atomic flag
        if (this.loaded.get())
        {
            return this.keys;
        }
        synchronized (this.synch)
        {
            // don't load twice
            if (this.loaded.get())
            {
                return this.keys;
            }
            try
            {
                // retrieve data
                final Object value = this.data.get();
                if (null == value)
                {
                    this.keys = new Object[0];
                    return this.keys;
                }
                if (!(value instanceof Collection))
                {
                    throw new IllegalArgumentException("Expected type of collection but found [" + value + "] from data holder [" + this.data + "].");
                }
                final Collection collection = (Collection) value;
                this.keys = collection.toArray();
                return this.keys;
            }
            catch (final Exception e)
            {
                throw new IllegalStateException("Unable to query lazy entity collection from data holder [" + this.data + "].", e);
            }
        }
    }


    @Override
    public boolean equals(final Object obj)
    {
        if (null == obj)
        {
            return false;
        }
        return this.ensureEntities().equals(obj);
    }


    @Override
    public int hashCode()
    {
        return this.ensureEntities().hashCode();
    }


    @Override
    public int size()
    {
        return this.ensureKeys().length;
    }


    @Override
    public boolean isEmpty()
    {
        return this.ensureKeys().length <= 0;
    }


    @Override
    public boolean contains(final Object o)
    {
        if (null == o)
        {
            return false;
        }
        return this.ensureEntities().contains(o);
    }


    @Override
    public Iterator<T> iterator()
    {
        return this.ensureEntities().iterator();
    }


    @Override
    public Object[] toArray()
    {
        return this.ensureEntities().toArray();
    }


    @Override
    public <T> T[] toArray(T[] a)
    {
        return this.ensureEntities().toArray(a);
    }


    @Override
    public boolean add(T o)
    {
        return this.ensureEntities().add(o);
    }


    @Override
    public boolean remove(Object o)
    {
        return this.ensureEntities().remove(o);
    }


    @Override
    public boolean containsAll(Collection<?> c)
    {
        return this.ensureEntities().containsAll(c);
    }


    @Override
    public boolean addAll(Collection<? extends T> c)
    {
        return this.ensureEntities().addAll(c);
    }


    @Override
    public boolean removeAll(Collection<?> c)
    {
        return this.ensureEntities().removeAll(c);
    }


    @Override
    public boolean retainAll(Collection<?> c)
    {
        return this.ensureEntities().retainAll(c);
    }


    @Override
    public void clear()
    {
        this.ensureEntities().clear();
    }
}
