package org.normandra.cassandra;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.normandra.NormandraException;
import org.normandra.data.DataHolder;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.DiscriminatorMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a lazy-loaded cassandra data holder that pulls entity whereValues
 * <p/>
 * User: bowen
 * Date: 4/5/14
 */
public class CassandraLazyKeyHolder implements DataHolder
{
    private final AtomicBoolean loaded = new AtomicBoolean(false);

    private final CassandraDatabaseSession session;

    private final EntityContext entity;

    private final TableMeta table;

    private final boolean collection;

    private final Map<String, Object> whereValues;

    private final List<Row> rows = new ArrayList<>();


    public CassandraLazyKeyHolder(final CassandraDatabaseSession session, final EntityContext meta, final TableMeta table, final boolean collection, final Map<String, Object> keys)
    {
        this.session = session;
        this.entity = meta;
        this.table = table;
        this.collection = collection;
        this.whereValues = new TreeMap<>(keys);
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
            throw new IllegalStateException("Unable to query lazy loaded results from table [" + this.table + "].", e);
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
            if (this.collection)
            {
                final List<Object> items = new ArrayList<>(rows.size());
                for (final Row row : rows)
                {
                    final Object item = CassandraUtils.unpackKey(row, this.entity);
                    if (item != null)
                    {
                        items.add(item);
                    }
                }
                return Collections.unmodifiableCollection(items);
            }
            else
            {
                return CassandraUtils.unpackKey(rows.get(0), this.entity);
            }
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to unpack lazy loaded results for entity [" + this.entity + "].", e);
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

            final Collection<ColumnMeta> keys = this.table.getPrimaryKeys();
            final List<String> names = new ArrayList<>(keys.size() + 1);
            for (final ColumnMeta column : keys)
            {
                names.add(column.getName());
            }
            for (final EntityMeta meta : this.entity.getEntities())
            {
                final DiscriminatorMeta descrim = meta.getDiscriminator();
                if (descrim != null)
                {
                    names.add(descrim.getColumn().getName());
                }
            }
            final String[] namesList = names.toArray(new String[names.size()]);

            final Select statement = QueryBuilder.select(namesList).from(this.session.getKeyspace(), this.table.getName());
            boolean hasWhere = false;
            for (final Map.Entry<String, Object> entry : this.whereValues.entrySet())
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
        }
        return this.rows;
    }
}
