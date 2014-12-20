package org.normandra.cassandra;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.normandra.NormandraException;
import org.normandra.data.DataHolder;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;
import org.normandra.util.ArraySet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a lazy-loaded cassandra data holder that pulls the value of a regularly
 * column
 * <p/>
 * User: bowen Date: 4/5/14
 */
public class CassandraLazyColumnHolder implements DataHolder
{
    private final AtomicBoolean loaded = new AtomicBoolean(false);

    private final CassandraDatabaseSession session;

    private final EntityMeta entity;

    private final TableMeta table;

    private final ColumnMeta column;

    private final Map<String, Object> keys;

    private final List<Row> rows = new ArrayList<>();

    public CassandraLazyColumnHolder(final CassandraDatabaseSession session, final EntityMeta meta, final TableMeta table, final ColumnMeta column, final Map<String, Object> keys)
    {
        this.session = session;
        this.entity = meta;
        this.table = table;
        this.column = column;
        this.keys = new TreeMap<>(keys);
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
            throw new IllegalStateException("Unable to query lazy loaded results from table [" + this.table + "] on column [" + this.column + "].", e);
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
            if (this.column.isEmbedded())
            {
                // embedded collection or simple value
                return CassandraUtils.unpackValue(rows.get(0), this.column);
            }
            else if (this.column.isCollection())
            {
                // join collection
                final Collection<Object> items = new ArraySet<>();
                for (final Row row : rows)
                {
                    final Object value = CassandraUtils.unpackValue(row, this.column);
                    if (value != null)
                    {
                        items.add(value);
                    }
                }
                return Collections.unmodifiableCollection(items);
            }
            else
            {
                // regular column
                return CassandraUtils.unpackValue(rows.get(0), this.column);
            }
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to toEntity lazy loaded results for entity [" + this.entity + "] on column [" + this.column + "].", e);
        }
    }

    private List<Row> ensureResults() throws NormandraException
    {
        if (this.loaded.get())
        {
            return this.rows;
        }

        final Select statement = QueryBuilder.select(this.column.getName()).from(this.session.getKeyspace(), this.table.getName());
        boolean hasWhere = false;
        for (final Map.Entry<String, Object> entry : this.keys.entrySet())
        {
            final String name = entry.getKey();
            final Object value = entry.getValue();
            if (value != null)
            {
                statement.where(QueryBuilder.eq(name, value));
                hasWhere = true;
            }
        }

        if (hasWhere)
        {
            this.rows.addAll(this.session.getSession().execute(statement).all());
        }
        this.loaded.getAndSet(true);
        return this.rows;
    }
}
