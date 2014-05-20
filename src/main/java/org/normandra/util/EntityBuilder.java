package org.normandra.util;

import org.normandra.DatabaseSession;
import org.normandra.NormandraException;
import org.normandra.data.BasicDataHolder;
import org.normandra.data.ColumnAccessor;
import org.normandra.data.DataHolder;
import org.normandra.data.DataHolderFactory;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;

import java.io.IOException;
import java.util.Map;

/**
 * a set of helper entity instance utilities
 * <p/>
 * User: bowen
 * Date: 5/15/14
 */
public class EntityBuilder
{
    private final DatabaseSession session;

    private final DataHolderFactory factory;


    public EntityBuilder(final DatabaseSession session, final DataHolderFactory factory)
    {
        this.session = session;
        this.factory = factory;
    }


    public final Object build(final EntityContext context, final Map<ColumnMeta, Object> data) throws NormandraException
    {
        if (null == data || data.isEmpty())
        {
            return null;
        }

        try
        {
            final EntityMeta entity = context.findEntity(data);
            if (null == entity)
            {
                return null;
            }
            return this.build(entity, data);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to build entity [" + context + "] from data map.", e);
        }
    }


    public final Object build(final EntityMeta entity, final Map<ColumnMeta, Object> data) throws IllegalAccessException, InstantiationException, NormandraException, IOException, ClassNotFoundException
    {
        if (null == data || data.isEmpty())
        {
            return null;
        }

        // create instance
        final Object instance = entity.getType().newInstance();
        if (null == instance)
        {
            return null;
        }
        if (!this.updateInstance(entity, instance, data))
        {
            return null;
        }

        // setup lazy loaded properties
        final Object key = entity.getId().fromEntity(instance);
        for (final TableMeta table : entity.getTables())
        {
            for (final ColumnMeta column : table)
            {
                if (column.isLazyLoaded())
                {
                    final ColumnAccessor accessor = entity.getAccessor(column);
                    if (accessor != null)
                    {
                        final DataHolder lazy = this.factory.createLazy(entity, table, column, key);
                        accessor.setValue(instance, lazy, this.session);
                    }
                }
            }
        }
        return instance;
    }


    private boolean updateInstance(final EntityMeta meta, final Object instance, final Map<ColumnMeta, Object> data) throws IOException, ClassNotFoundException, NormandraException
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
                final DataHolder placeholder = new BasicDataHolder(value);
                accessor.setValue(instance, placeholder, this.session);
                updated = true;
            }
        }
        return updated;
    }
}
