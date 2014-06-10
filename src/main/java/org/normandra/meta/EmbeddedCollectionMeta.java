package org.normandra.meta;

import org.apache.commons.lang.NullArgumentException;

/**
 * collection column meta-data
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
public class EmbeddedCollectionMeta extends ColumnMeta
{
    private final Class<?> generic;


    public EmbeddedCollectionMeta(final String name, final String property, final Class<?> clazz, final Class<?> generic, final boolean primary, final boolean lazy)
    {
        super(name, property, clazz, primary, lazy);
        if (null == generic)
        {
            throw new NullArgumentException("generic");
        }
        this.generic = generic;
    }


    @Override
    public boolean isCollection()
    {
        return true;
    }


    @Override
    public boolean isEmbedded()
    {
        return true;
    }


    public Class<?> getGeneric()
    {
        return this.generic;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        EmbeddedCollectionMeta that = (EmbeddedCollectionMeta) o;

        if (generic != null ? !generic.equals(that.generic) : that.generic != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (generic != null ? generic.hashCode() : 0);
        return result;
    }
}
