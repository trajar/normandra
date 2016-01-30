package org.normandra.util;

import org.apache.commons.lang.NullArgumentException;
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
import org.normandra.meta.TableMeta;

import java.util.Map;

/**
 * a set of helper entity instance utilities
 * <p>
 * Date: 5/15/14
 */
public class EntityBuilder
{
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

    public Object build(final EntityMeta meta, final Map<ColumnMeta, Object> data) throws NormandraException
    {
        if (null == meta)
        {
            throw new NullArgumentException("entity meta");
        }
        if (null == data || data.isEmpty())
        {
            return null;
        }

        // create instance
        final Object instance;
        try
        {
            instance = meta.getType().newInstance();
            if (null == instance)
            {
                return null;
            }
            if (!this.update(meta, instance, data))
            {
                return null;
            }
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to construct instance for entity [" + meta + "].", e);
        }

        // setup lazy loaded properties
        final Object key = meta.getId().fromEntity(instance);
        if (null == key)
        {
            return null;
        }
        for (final TableMeta table : meta.getTables())
        {
            for (final ColumnMeta column : table)
            {
                if (!data.containsKey(column))
                {
                    final ColumnAccessor accessor = meta.getAccessor(column);
                    if (accessor != null && !accessor.isLoaded(instance))
                    {
                        DataHolder lazy = null;
                        if (column instanceof JoinColumnMeta)
                        {
                            lazy = this.factory.createJoinColumn(meta, table, (JoinColumnMeta) column, key);
                        }
                        else if (column instanceof JoinCollectionMeta)
                        {
                            lazy = this.factory.createJoinCollection(meta, table, (JoinCollectionMeta) column, key);
                        }
                        else if (column instanceof MappedColumnMeta)
                        {
                            lazy = this.factory.createMappedColumn(meta, (MappedColumnMeta) column, key);
                        }
                        else if (!column.isEmbedded() && !data.containsKey(column))
                        {
                            lazy = this.factory.createLazy(meta, table, column, key);
                        }
                        if (lazy != null)
                        {
                            accessor.setValue(instance, lazy, this.session);
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
            final Object value = entry.getValue();
            final ColumnAccessor accessor = meta.getAccessor(column);
            if (value != null && accessor != null)
            {
                final DataHolder placeholder = this.factory.createStatic(value);
                if (placeholder != null)
                {
                    accessor.setValue(instance, placeholder, this.session);
                    updated = true;
                }
            }
        }
        return updated;
    }
}