package org.normandra.util;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * an ArrayList implementation of Set
 *
 * @param <E>
 */
public class ArraySet<E> extends AbstractSet<E>
{
    protected final ArrayList<E> items;


    public ArraySet()
    {
        this(10);
    }


    public ArraySet(final Collection<? extends E> collection)
    {
        items = new ArrayList<>(collection.size());
        for (E item : collection)
        {
            if (!items.contains(item))
            {
                items.add(item);
            }
        }
    }


    public ArraySet(final int initialCapacity)
    {
        items = new ArrayList<>(initialCapacity);
    }


    @Override
    public boolean add(final E item)
    {
        if (null == item)
        {
            return false;
        }
        if (items.contains(item))
        {
            return false;
        }
        else
        {
            return items.add(item);
        }
    }


    public E get(final int index) throws IndexOutOfBoundsException
    {
        return items.get(index);
    }


    @Override
    public Iterator<E> iterator()
    {
        return items.iterator();
    }


    @Override
    public int size()
    {
        return items.size();
    }
}
