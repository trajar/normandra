package org.normandra.util;

import org.apache.commons.lang.NullArgumentException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * a lazy-loaded collection backed by an iterator
 * <p>
 * User: bowen
 * Date: 6/29/14
 */
public class LazyCollection<T> implements Collection<T>
{
    private final Iterator<T> iterator;

    private final List<T> list = new ArrayList<>();


    public LazyCollection(final Iterator<T> itr)
    {
        if (null == itr)
        {
            throw new NullArgumentException("iterator");
        }
        this.iterator = itr;
    }


    public List<T> subset(final int index, final int count)
    {
        if (index < 0)
        {
            return Collections.emptyList();
        }
        this.readTo(index);
        final int size = this.list.size();
        final int toIndex = Math.min(index + count, this.list.size() - 1);
        if (index >= size)
        {
            throw new IllegalArgumentException("Index [" + index + "] is greater than size [" + size + "] of collection.");
        }
        return this.list.subList(index, toIndex);
    }


    @Override
    public Iterator<T> iterator()
    {
        return new Iterator<T>()
        {
            int index = 0;


            @Override
            public boolean hasNext()
            {
                readNext();
                if (list.isEmpty())
                {
                    return false;
                }
                return index < list.size();
            }


            @Override
            public T next()
            {
                final T item = list.get(index);
                index++;
                return item;
            }
        };
    }


    @Override
    public int size()
    {
        this.readAll();
        return this.list.size();
    }


    @Override
    public boolean isEmpty()
    {
        this.readTo(1);
        return this.list.isEmpty();
    }


    @Override
    public boolean contains(final Object o)
    {
        this.readAll();
        return this.list.contains(o);
    }


    @Override
    public boolean containsAll(Collection<?> c)
    {
        this.readAll();
        return this.list.containsAll(c);
    }


    @Override
    public Object[] toArray()
    {
        this.readAll();
        return this.list.toArray();
    }


    @Override
    public <T1> T1[] toArray(T1[] a)
    {
        this.readAll();
        return this.list.toArray(a);
    }


    @Override
    public boolean add(T t)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean remove(Object o)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean addAll(Collection<? extends T> c)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean removeAll(Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean retainAll(Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void clear()
    {
        throw new UnsupportedOperationException();
    }


    private int readAll()
    {
        int read = 0;
        while (this.iterator.hasNext())
        {
            final T item = this.iterator.next();
            if (item != null)
            {
                this.list.add(item);
                read++;
            }
        }
        return read;
    }


    private boolean readNext()
    {
        if (!this.iterator.hasNext())
        {
            return false;
        }
        final T item = this.iterator.next();
        if (item != null)
        {
            return this.list.add(item);
        }
        else
        {
            return false;
        }
    }


    private int readTo(final int index)
    {
        if (index < 0)
        {
            return 0;
        }
        int read = 0;
        while (this.iterator.hasNext() && this.list.size() <= index)
        {
            final T item = this.iterator.next();
            if (item != null)
            {
                this.list.add(item);
                read++;
            }
        }
        return read;
    }
}
