package org.normandra.data;

/**
 * a data holder wrapped a fixed / concrete value
 * User: bowen
 * Date: 3/24/14
 */
public class BasicDataHolder implements DataHolder
{
    private final Object value;


    public BasicDataHolder(final Object obj)
    {
        this.value = obj;
    }


    @Override
    public boolean isEmpty()
    {
        return null == this.value;
    }


    @Override
    public Object get()
    {
        return this.value;
    }


    @Override
    public String toString()
    {
        if (this.value != null)
        {
            return "Data{" + this.value + "}";
        }
        else
        {
            return "Data{null}";
        }
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BasicDataHolder that = (BasicDataHolder) o;

        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        return value != null ? value.hashCode() : 0;
    }
}
