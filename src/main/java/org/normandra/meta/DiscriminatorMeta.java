package org.normandra.meta;

import org.normandra.data.ReadOnlyColumnAccessor;

/**
 * a discriminator column for abstract/inherited entities
 * <p/>
 * User: bowen
 * Date: 2/1/14
 */
public class DiscriminatorMeta<T> extends ColumnMeta<T>
{
    private final Object value;


    public DiscriminatorMeta(final String name, final String property, final T descrim, final Class<T> clazz)
    {
        super(name, property, new ReadOnlyColumnAccessor(descrim), clazz, false);
        this.value = descrim;
    }


    public Object getValue()
    {
        return this.value;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        DiscriminatorMeta that = (DiscriminatorMeta) o;

        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
