package org.normandra.meta;

import org.apache.commons.lang.NullArgumentException;

/**
 * a join column meta description
 * <p>
 * User: bowen
 * Date: 2/14/14
 */
public class JoinColumnMeta extends ColumnMeta
{
    private final EntityContext entity;


    public JoinColumnMeta(final String name, final String property, final Class<?> clazz, final EntityContext entity, final boolean primaryKey)
    {
        super(name, property, clazz, primaryKey, false);
        if (null == entity)
        {
            throw new NullArgumentException("entity");
        }
        this.entity = entity;
    }


    public EntityContext getEntity()
    {
        return this.entity;
    }
}
