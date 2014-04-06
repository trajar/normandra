package org.normandra.cassandra;

import com.datastax.driver.core.Row;
import org.normandra.NormandraException;
import org.normandra.data.ColumnAccessor;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;

import java.io.IOException;
import java.util.Map;

/**
 * entity builder api
 * <p>
 * User: bowen
 * Date: 4/5/14
 */
public class CassandraEntityBuilder
{
    private final CassandraDatabaseSession session;


    public CassandraEntityBuilder(final CassandraDatabaseSession session)
    {
        this.session = session;
    }


    public Object build(final EntityContext context, final Map<ColumnMeta, Object> data) throws NormandraException
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


    public Object build(final EntityContext context, final Row row) throws NormandraException
    {
        if (null == row)
        {
            return null;
        }

        try
        {
            final TableMeta table = context.findTable(row.getColumnDefinitions().getTable(0));
            final Map<ColumnMeta, Object> data = CassandraUtils.unpackValues(table, row);
            final EntityMeta entity = context.findEntity(data);
            if (null == entity)
            {
                return null;
            }
            return this.build(entity, data);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to build entity [" + context + "] from row [" + row + "].", e);
        }
    }


    private Object build(final EntityMeta entity, final Map<ColumnMeta, Object> data) throws IllegalAccessException, InstantiationException, NormandraException, IOException, ClassNotFoundException
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
        if (!CassandraUtils.updateInstance(entity, instance, data, this.session))
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
                        accessor.setValue(instance, new LazyDataHolder(this.session, entity, table, column, key), this.session);
                    }
                }
            }
        }
        return instance;
    }
}
