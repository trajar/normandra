package org.normandra.association;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.EntitySession;
import org.normandra.data.DataHolder;
import org.normandra.util.ArraySet;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * lazy loaded element collection
 * <p>
 * User: bowen
 * Date: 3/29/14
 */
public class LazyElementCollection<T> implements Set<T>, LazyLoadedCollection<T>
{
    private final DataHolder data;

    private final EntitySession session;

    private Collection<T> entities;

    private final Object synch = new Object();

    private final AtomicBoolean loaded = new AtomicBoolean(false);


    public LazyElementCollection(final EntitySession session, final DataHolder data)
    {
        if (null == data)
        {
            throw new NullArgumentException("data holder");
        }
        if (null == session)
        {
            throw new NullArgumentException("session");
        }
        this.data = data;
        this.session = session;
    }


    @Override
    public boolean isLoaded()
    {
        return this.loaded.get();
    }


    @Override
    public LazyLoadedCollection<T> duplicate()
    {
        return new LazyElementCollection<>(session, data);
    }


    private Collection ensureEntities()
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
                    this.entities = new ArraySet<>(0);
                }
                else if (value instanceof Collection)
                {
                    this.entities = new HashSet<T>((Collection) value);
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
        return this.ensureEntities().equals(obj);
    }


    @Override
    public int size()
    {
        return this.ensureEntities().size();
    }


    @Override
    public boolean isEmpty()
    {
        return this.ensureEntities().isEmpty();
    }


    @Override
    public boolean contains(final Object o)
    {
        if (null == o)
        {

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
        return (T[]) this.ensureEntities().toArray(a);
    }


    @Override
    public boolean add(Object o)
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
    public boolean addAll(Collection c)
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
