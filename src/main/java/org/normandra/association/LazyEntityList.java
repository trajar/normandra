package org.normandra.association;

import org.normandra.EntitySession;
import org.normandra.data.DataHolder;
import org.normandra.meta.EntityContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

/**
 * User: bowen
 * Date: 3/27/14
 */
public class LazyEntityList<T> extends LazyEntityCollection<T> implements List<T>
{
    public LazyEntityList(EntitySession session, EntityContext meta, DataHolder data, ElementIdentity<T> factory)
    {
        super(session, meta, data, factory, new CollectionFactory<T>()
        {
            @Override
            public Collection<T> create(int size)
            {
                return new ArrayList<>(size);
            }
        });
    }


    @Override
    public LazyEntityList<T> duplicate()
    {
        return new LazyEntityList<>(this.session, this.entity, this.data, this.elementFactory);
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
