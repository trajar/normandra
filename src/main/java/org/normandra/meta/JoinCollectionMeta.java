package org.normandra.meta;

/**
 * joined collection column, usually from one-to-many association
 * <p>
 * User: bowen
 * Date: 3/22/14
 */
public class JoinCollectionMeta extends ColumnMeta
{
    private final EntityContext entity;

    private final boolean embedded;


    public JoinCollectionMeta(final String name, final String property, final Class<?> clazz, final EntityContext associated, final boolean primary, final boolean lazy, final boolean embedded)
    {
        super(name, property, clazz, primary, lazy);
        this.entity = associated;
        this.embedded = embedded;
    }


    @Override
    public boolean isCollection()
    {
        return true;
    }


    @Override
    public boolean isEmbedded()
    {
        return this.embedded;
    }


    public EntityContext getEntity()
    {
        return this.entity;
    }
}
