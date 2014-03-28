package org.normandra.association;

import org.normandra.EntitySession;
import org.normandra.data.DataHolder;
import org.normandra.meta.EntityMeta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

/**
 * User: bowen
 * Date: 3/27/14
 */
public class LazyEntityList extends LazyEntityCollection implements List<Object>
{
    public LazyEntityList(EntitySession session, EntityMeta meta, DataHolder data)
    {
        super(session, meta, data);
    }


    @Override
    protected Collection<Object> createCollection()
    {
        return new ArrayList<>();
    }


    @Override
    protected List<Object> getCollection()
    {
        return (List) super.getCollection();
    }


    @Override
    public boolean addAll(int index, Collection<?> c)
    {
        return this.getCollection().addAll(index, c);
    }


    @Override
    public Object get(int index)
    {
        return this.getCollection().get(index);
    }


    @Override
    public Object set(int index, Object element)
    {
        return this.getCollection().set(index, element);
    }


    @Override
    public void add(int index, Object element)
    {
        this.getCollection().add(index, element);
    }


    @Override
    public Object remove(int index)
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
    public ListIterator<Object> listIterator()
    {
        return this.getCollection().listIterator();
    }


    @Override
    public ListIterator<Object> listIterator(int index)
    {
        return this.getCollection().listIterator(index);
    }


    @Override
    public List<Object> subList(int fromIndex, int toIndex)
    {
        return this.getCollection().subList(fromIndex, toIndex);
    }
}
