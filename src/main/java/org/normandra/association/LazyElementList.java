package org.normandra.association;

import org.normandra.EntitySession;
import org.normandra.data.DataHolder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

/**
 * an element collection list
 * <p>
 * User: bowen
 * Date: 3/30/14
 */
public class LazyElementList<T> extends LazyElementCollection<T> implements List<T>
{
    public LazyElementList(EntitySession session, DataHolder data)
    {
        super(session, data, new CollectionFactory<T>()
        {
            @Override
            public Collection<T> create(int size)
            {
                return new ArrayList<>(size);
            }
        });
    }


    @Override
    public LazyLoadedCollection<T> duplicate()
    {
        return new LazyElementList<>(this.session, this.data);
    }


    @Override
    protected List<T> getCollection()
    {
        return (List) super.getCollection();
    }


    @Override
    public boolean addAll(int index, Collection<? extends T> c)
    {
        return this.getCollection().addAll(index, c);
    }


    @Override
    public T get(int index)
    {
        return this.getCollection().get(index);
    }


    @Override
    public T set(int index, T element)
    {
        return this.getCollection().set(index, element);
    }


    @Override
    public void add(int index, T element)
    {
        this.getCollection().add(index, element);
    }


    @Override
    public T remove(int index)
    {
        return this.getCollection().remove(index);
    }


    @Override
    public int indexOf(Object o)
    {
        return this.getCollection().indexOf(o);
    }


    @Override
    public int lastIndexOf(Object o)
    {
        return this.getCollection().lastIndexOf(o);
    }


    @Override
    public ListIterator<T> listIterator()
    {
        return this.getCollection().listIterator();
    }


    @Override
    public ListIterator<T> listIterator(int index)
    {
        return this.getCollection().listIterator(index);
    }


    @Override
    public List<T> subList(int fromIndex, int toIndex)
    {
        return this.getCollection().subList(fromIndex, toIndex);
    }
}
