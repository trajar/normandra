package org.normandra.meta;

/**
 * joined collection column, usually from one-to-many association
 * <p>
 * User: bowen
 * Date: 3/22/14
 */
public class JoinCollectionMeta extends ColumnMeta
{
    private final EntityMeta entity;


    public JoinCollectionMeta(final String name, final String property, final Class<?> clazz, final EntityMeta associated, final boolean primary, final boolean lazy)
    {
        super(name, property, clazz, primary, lazy);
        this.entity = associated;
    }


    public EntityMeta getEntity()
    {
        return this.entity;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        JoinCollectionMeta that = (JoinCollectionMeta) o;

        if (entity != null ? !entity.equals(that.entity) : that.entity != null) return false;

        return true;
    }


    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (entity != null ? entity.hashCode() : 0);
        return result;
    }
}
