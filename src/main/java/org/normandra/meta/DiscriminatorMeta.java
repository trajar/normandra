package org.normandra.meta;

import java.io.Serializable;

/**
 * a discriminator column for abstract/inherited entities
 * <p>
 * 
 * Date: 2/1/14
 */
public class DiscriminatorMeta
{
    private final Object value;

    private final ColumnMeta column;


    public DiscriminatorMeta(final ColumnMeta column, final Serializable descrim)
    {
        this.column = column;
        this.value = descrim;
    }


    public ColumnMeta getColumn()
    {
        return this.column;
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

        DiscriminatorMeta that = (DiscriminatorMeta) o;

        if (column != null ? !column.equals(that.column) : that.column != null) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + (column != null ? column.hashCode() : 0);
        return result;
    }
}
