package org.normandra.data;

import org.normandra.DatabaseSession;
import org.normandra.NormandraException;
import org.normandra.association.AssociationUtils;
import org.normandra.meta.EntityMeta;

import java.lang.reflect.Field;

/**
 * a data accessor for a single row (one to one, many to one)
 * <p/>
 * User: bowen
 * Date: 2/1/14
 */
public class JoinColumnAccessor extends FieldColumnAccessor implements ColumnAccessor
{
    private final EntityMeta entity;

    private final boolean lazy;


    public JoinColumnAccessor(final Field field, final EntityMeta meta, final boolean lazy)
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
            return this.get(entity) != null;
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get join-column.", e);
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
    public boolean setValue(final Object entity, final Object key, final DatabaseSession session) throws NormandraException
    {
        if (null == key)
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
            try
            {
                final Object associated;
                if (this.lazy)
                {
                    associated = AssociationUtils.proxy(this.entity, key, session);
                }
                else
                {
                    associated = session.get(this.entity, key);
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