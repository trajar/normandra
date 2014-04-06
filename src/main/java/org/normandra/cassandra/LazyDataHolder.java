package org.normandra.cassandra;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.normandra.NormandraException;
import org.normandra.data.DataHolder;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a lazy-loaded cassandra data holder
 * <p>
 * User: bowen
 * Date: 4/5/14
 */
public class LazyDataHolder implements DataHolder
{
    private final AtomicBoolean loaded = new AtomicBoolean(false);

    private final CassandraDatabaseSession session;

    private final EntityMeta entity;

    private final TableMeta table;

    private final ColumnMeta column;

    private final Object key;

    private final List<Row> rows = new ArrayList<>();


    public LazyDataHolder(final CassandraDatabaseSession session, final EntityMeta meta, final TableMeta table, final ColumnMeta column, final Object key)
    {
        this.session = session;
        this.entity = meta;
        this.table = table;
        this.column = column;
        this.key = key;
    }


    @Override
    public boolean isEmpty()
    {
        try
        {
            return this.ensureResults().isEmpty();
        }
        catch (final Exception e)
        {
            throw new IllegalStateException("Unable to query lazy loaded results from table [" + this.table + "] column [" + this.column + "].", e);
        }
    }


    @Override
    public Object get() throws NormandraException
    {
        final List<Row> rows = this.ensureResults();
        if (null == rows || rows.isEmpty())
        {
            return null;
        }
        try
        {
            return CassandraUtils.unpackValue(rows, this.column.getName(), this.column);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to unpack lazy loaded results for column [" + this.column + "] on entity [" + this.entity + "].", e);
        }
    }


    private List<Row> ensureResults() throws NormandraException
    {
        if (this.loaded.get())
        {
            return this.rows;
        }
        synchronized (this)
        {
            if (this.loaded.get())
            {
                return this.rows;
            }
            final Select statement = QueryBuilder.select(this.column.getName()).from(this.session.getKeyspace(), this.table.getName());
            boolean hasWhere = false;
            for (final Map.Entry<String, Object> entry : this.entity.getId().fromKey(this.key).entrySet())
            {
                final String name = entry.getKey();
                final Object value = entry.getValue();
                statement.where(QueryBuilder.eq(name, value));
                hasWhere = true;
            }
            if (hasWhere)
            {
                this.rows.addAll(this.session.getSession().execute(statement).all());
            }
            this.loaded.getAndSet(true);
        }
        return this.rows;
    }
}
