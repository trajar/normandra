package org.normandra.association;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.EntitySession;
import org.normandra.data.DataHolder;
import org.normandra.meta.EntityMeta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a lazy loaded entity collection
 * <p>
 * User: bowen
 * Date: 3/25/14
 */
abstract public class LazyEntityCollection implements Collection<Object>
{
    private final DataHolder data;

    private final EntityMeta entity;

    private final EntitySession session;

    private Object[] keys;

    private Collection<Object> entities;

    private final Object synch = new Object();

    private final AtomicBoolean loaded = new AtomicBoolean(false);


    public LazyEntityCollection(final EntitySession session, final EntityMeta meta, final DataHolder data)
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
        this.data = data;
        this.session = session;
        this.entity = meta;
    }


    abstract protected Collection<Object> createCollection();


    public EntityMeta getEntity()
    {
        return this.entity;
    }


    protected Collection<Object> getCollection()
    {
        return this.ensureEntities();
    }


    private Collection<Object> ensureEntities()
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
                this.entities = new ArrayList<>();
                return this.entities;
            }
            try
            {
                this.entities = this.session.get(this.entity, keys);
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

        }
        return this.ensureEntities().contains(o);
    }


    @Override
    public Iterator<Object> iterator()
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
    public boolean addAll(Collection<?> c)
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
