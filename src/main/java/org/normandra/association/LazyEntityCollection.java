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
 * <p>
 * User: bowen
 * Date: 3/25/14
 */
abstract public class LazyEntityCollection<T> implements LazyLoadedCollection<T>
{
    protected final DataHolder data;

    protected final EntityContext entity;

    protected final EntitySession session;

    protected final CollectionFactory<T> collectionFactory;

    protected final ElementFactory<T> elementFactory;

    private Object[] keys;

    private Collection<T> entities;

    private final Object synch = new Object();

    private final AtomicBoolean loaded = new AtomicBoolean(false);


    public LazyEntityCollection(final EntitySession session, final EntityContext meta, final DataHolder data, ElementFactory<T> ef, final CollectionFactory<T> cf)
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
        if (null == ef)
        {
            throw new NullArgumentException("element factory");
        }
        if (null == cf)
        {
            throw new NullArgumentException("collection factory");
        }
        this.data = data;
        this.session = session;
        this.entity = meta;
        this.elementFactory = ef;
        this.collectionFactory = cf;
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

        synchronized (this.synch)
        {
            try
            {
                final Object value = this.data.get();
                if (null == value)
                {
                    this.keys = new Object[0];
                }
                else
                {
                    if (!(value instanceof Collection))
                    {
                        throw new IllegalArgumentException("Expected type of collection but found [" + value + "] from data holder [" + this.data + "].");
                    }
                    final Collection collection = (Collection) value;
                    this.keys = collection.toArray();
                }
                this.loaded.getAndSet(true);
                if (this.keys.length <= 0)
                {
                    this.entities = this.collectionFactory.create(0);
                    return this.entities;
                }
                else
                {
                    final List<T> results = this.elementFactory.unpack(this.session, this.keys);
                    this.entities = this.collectionFactory.create(results.size());
                    this.entities.addAll(results);
                    return this.entities;
                }
            }
            catch (final Exception e)
            {
                throw new IllegalStateException("Unable to query lazy entity collection with data [" + this.data + "].", e);
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
