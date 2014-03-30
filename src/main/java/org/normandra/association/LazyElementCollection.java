package org.normandra.association;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.EntitySession;
import org.normandra.data.DataHolder;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * lazy loaded element collection
 * <p>
 * User: bowen
 * Date: 3/29/14
 */
abstract public class LazyElementCollection<T> implements LazyLoadedCollection<T>
{
    protected final DataHolder data;

    protected final EntitySession session;

    private final CollectionFactory<T> factory;

    private Collection<T> entities;

    private final Object synch = new Object();

    private final AtomicBoolean loaded = new AtomicBoolean(false);


    public LazyElementCollection(final EntitySession session, final DataHolder data, final CollectionFactory<T> factory)
    {
        if (null == data)
        {
            throw new NullArgumentException("data holder");
        }
        if (null == session)
        {
            throw new NullArgumentException("session");
        }
        if (null == factory)
        {
            throw new NullArgumentException("collection factory");
        }
        this.data = data;
        this.session = session;
        this.factory = factory;
    }


    @Override
    public boolean isLoaded()
    {
        return this.loaded.get();
    }


    protected Collection<T> getCollection()
    {
        return this.ensureCollection();
    }


    private Collection<T> ensureCollection()
    {
        if (this.loaded.get())
        {
            return this.entities;
        }

        synchronized (this.synch)
        {
            try
            {
                final Object value = this.data.get();
                if (null == value)
                {
                    this.entities = this.factory.create(0);
                }
                else if (value instanceof Collection)
                {
                    final Collection<T> collection = (Collection) value;
                    this.entities = this.factory.create(collection.size());
                    this.entities.addAll(collection);
                }
                else
                {
                    throw new IllegalStateException("Expected element collection but found value [" + value + "].");
                }
                this.loaded.getAndSet(true);
                return this.entities;
            }
            catch (final Exception e)
            {
                throw new IllegalStateException("Unable to query lazy element collection with data [" + this.data + "].", e);
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
        return this.ensureCollection().equals(obj);
    }


    @Override
    public int hashCode()
    {
        return this.ensureCollection().hashCode();
    }


    @Override
    public int size()
    {
        return this.ensureCollection().size();
    }


    @Override
    public boolean isEmpty()
    {
        return this.ensureCollection().isEmpty();
    }


    @Override
    public boolean contains(final Object o)
    {
        if (null == o)
        {

        }
        return this.ensureCollection().contains(o);
    }


    @Override
    public Iterator<T> iterator()
    {
        return this.ensureCollection().iterator();
    }


    @Override
    public Object[] toArray()
    {
        return this.ensureCollection().toArray();
    }


    @Override
    public <T> T[] toArray(T[] a)
    {
        return (T[]) this.ensureCollection().toArray(a);
    }


    @Override
    public boolean add(T o)
    {
        return this.ensureCollection().add(o);
    }


    @Override
    public boolean remove(Object o)
    {
        return this.ensureCollection().remove(o);
    }


    @Override
    public boolean containsAll(Collection<?> c)
    {
        return this.ensureCollection().containsAll(c);
    }


    @Override
    public boolean addAll(Collection c)
    {
        return this.ensureCollection().addAll(c);
    }


    @Override
    public boolean removeAll(Collection<?> c)
    {
        return this.ensureCollection().removeAll(c);
    }


    @Override
    public boolean retainAll(Collection<?> c)
    {
        return this.ensureCollection().retainAll(c);
    }


    @Override
    public void clear()
    {
        this.ensureCollection().clear();
    }
}
