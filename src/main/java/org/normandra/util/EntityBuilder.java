package org.normandra.util;

import org.normandra.EntitySession;
import org.normandra.NormandraException;
import org.normandra.data.ColumnAccessor;
import org.normandra.data.DataHolder;
import org.normandra.data.DataHolderFactory;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.JoinCollectionMeta;
import org.normandra.meta.JoinColumnMeta;
import org.normandra.meta.MappedColumnMeta;
import org.normandra.meta.SingleEntityContext;
import org.normandra.meta.TableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * a set of helper entity instance utilities
 * <p>
 * User: bowen
 * Date: 5/15/14
 */
public class EntityBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(EntityBuilder.class);

    private final EntitySession session;

    private final DataHolderFactory factory;


    public EntityBuilder(final EntitySession session, final DataHolderFactory factory)
    {
        this.session = session;
        this.factory = factory;
    }


    public Object build(final EntityContext context, final Map<ColumnMeta, Object> data) throws NormandraException
    {
        if (null == data || data.isEmpty())
        {
            return null;
        }

        final EntityMeta entity = context.findEntity(data);
        if (null == entity)
        {
            return null;
        }
        return this.build(entity, data);
    }


    public Object build(final EntityMeta entity, final Map<ColumnMeta, Object> data) throws NormandraException
    {
        if (null == data || data.isEmpty())
        {
            return null;
        }

        // create instance
        final Object instance;
        try
        {
            instance = entity.getType().newInstance();
            if (null == instance)
            {
                return null;
            }
            if (!this.update(entity, instance, data))
            {
                return null;
            }
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to construct instance for entity [" + entity + "].", e);
        }

        // setup lazy loaded properties
        final Object key = entity.getId().fromEntity(instance);
        if (null == key)
        {
            return null;
        }
        for (final TableMeta table : entity.getTables())
        {
            for (final ColumnMeta column : table)
            {
                if (column.isLazyLoaded())
                {
                    final ColumnAccessor accessor = entity.getAccessor(column);
                    if (accessor != null && !accessor.isLoaded(instance))
                    {
                        final EntityContext context = new SingleEntityContext(entity);
                        final DataHolder lazy;
                        if (column instanceof JoinColumnMeta)
                        {
                            lazy = this.factory.createJoinColumn(context, table, (JoinColumnMeta) column, key);
                        }
                        else if (column instanceof JoinCollectionMeta)
                        {
                            lazy = this.factory.createJoinCollection(context, table, (JoinCollectionMeta) column, key);
                        }
                        else if (column instanceof MappedColumnMeta)
                        {
                            lazy = this.factory.createMappedColumn(context, (MappedColumnMeta) column, key);
                        }
                        else
                        {
                            lazy = this.factory.createLazy(new SingleEntityContext(entity), table, column, key);
                        }
                        if (lazy != null)
                        {
                            accessor.setValue(instance, lazy, this.session);
                        }
                        else
                        {
                            logger.warn("Unable to create data holder for [" + column + "] on entity [" + entity + "].");
                        }
                    }
                }
            }
        }
        return instance;
    }


    public boolean update(final EntityMeta meta, final Object instance, final Map<ColumnMeta, Object> data) throws NormandraException
    {
        if (null == data || data.isEmpty())
        {
            return false;
        }
        boolean updated = false;
        for (final Map.Entry<ColumnMeta, Object> entry : data.entrySet())
        {
            final ColumnMeta column = entry.getKey();
            final ColumnAccessor accessor = meta.getAccessor(column);
            if (accessor != null)
            {
                final Object value = entry.getValue();
                final DataHolder placeholder = this.factory.createStatic(value);
                accessor.setValue(instance, placeholder, this.session);
                updated = true;
            }
        }
        return updated;
    }
}
