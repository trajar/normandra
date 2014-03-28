package org.normandra.meta;

import org.apache.commons.lang.NullArgumentException;

/**
 * a join column meta description
 * <p/>
 * User: bowen
 * Date: 2/14/14
 */
public class JoinColumnMeta extends ColumnMeta
{
    private final EntityMeta entity;


    public JoinColumnMeta(final String name, final String property, final Class<?> clazz, final EntityMeta entity, final boolean primaryKey)
    {
        super(name, property, clazz, primaryKey);
        if (null == entity)
        {
            throw new NullArgumentException("entity");
        }
        this.entity = entity;
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

        JoinColumnMeta that = (JoinColumnMeta) o;

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
