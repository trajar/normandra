package org.normandra.data;

import org.normandra.EntitySession;
import org.normandra.NormandraException;
import org.normandra.association.AssociationUtils;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.SingleEntityContext;

import java.lang.reflect.Field;

/**
 * a data accessor for a single join association
 * <p>
 * User: bowen
 * Date: 2/1/14
 */
public class SingleJoinColumnAccessor extends FieldColumnAccessor implements ColumnAccessor
{
    private final EntityMeta entity;

    private final boolean lazy;


    public SingleJoinColumnAccessor(final Field field, final EntityMeta meta, final boolean lazy)
    {
        super(field);
        this.entity = meta;
        this.lazy = lazy;
    }


    @Override
    public boolean isEmpty(final Object entity) throws NormandraException
    {
        try
        {
            return this.get(entity) == null;
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get join-column.", e);
        }
    }


    @Override
    public boolean isLoaded(final Object entity) throws NormandraException
    {
        try
        {
            final Object association = this.get(entity);
            if (null == association)
            {
                return true;
            }
            if (!AssociationUtils.isProxy(association))
            {
                return true;
            }
            return AssociationUtils.isLoaded(association);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to determine if entity/proxy column [" + this.getField().getName() + "] is loaded.", e);
        }
    }


    @Override
    public Object getValue(final Object entity) throws NormandraException
    {
        final Object associatedEntity;
        try
        {
            associatedEntity = this.get(entity);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get join-column [" + this.getField().getName() + "].", e);
        }
        if (null == associatedEntity)
        {
            return null;
        }
        return this.entity.getId().fromEntity(associatedEntity);
    }


    @Override
    public boolean setValue(final Object entity, final DataHolder data, final EntitySession session) throws NormandraException
    {
        if (null == data || data.isEmpty())
        {
            try
            {
                this.set(entity, null);
                return true;
            }
            catch (final Exception e)
            {
                throw new NormandraException("Unable to set join-column [" + this.getField().getName() + "] to empty/null value.", e);
            }
        }
        else
        {
            final Object key = data.get();
            try
            {
                final Object associated;
                if (this.lazy)
                {
                    associated = AssociationUtils.createProxy(this.entity, key, session);
                }
                else
                {
                    associated = session.get(new SingleEntityContext(this.entity), key);
                }
                this.set(entity, associated);
                return true;
            }
            catch (final Exception e)
            {
                throw new NormandraException("Unable to set join-column [" + this.getField().getName() + "] from key [" + key + "].", e);
            }
        }
    }
}