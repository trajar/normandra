package org.normandra.cassandra;

import com.datastax.driver.core.Row;
import org.normandra.NormandraException;
import org.normandra.data.BasicDataHolder;
import org.normandra.data.DataHolder;
import org.normandra.data.DataHolderFactory;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;
import org.normandra.util.EntityBuilder;

import java.util.Map;

/**
 * entity builder api
 * <p/>
 * User: bowen
 * Date: 4/5/14
 */
public class CassandraEntityBuilder extends EntityBuilder
{
    public CassandraEntityBuilder(final CassandraDatabaseSession session)
    {
        super(session, new DataHolderFactory()
        {
            @Override
            public DataHolder createStatic(Object value)
            {
                return new BasicDataHolder(value);
            }


            @Override
            public DataHolder createLazy(EntityMeta meta, TableMeta table, ColumnMeta column, Object key)
            {
                return new LazyDataHolder(session, meta, table, column, key);
            }
        });
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
}
