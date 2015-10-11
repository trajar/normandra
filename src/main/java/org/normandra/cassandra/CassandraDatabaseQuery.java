package org.normandra.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.DatabaseQuery;
import org.normandra.NormandraException;
import org.normandra.log.DatabaseActivity;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;
import org.normandra.util.EntityBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * cassandra database query
 * <p>
 *  Date: 4/5/14
 */
public class CassandraDatabaseQuery<T> implements DatabaseQuery<T>
{
    private final CassandraDatabaseSession session;

    private final EntityContext context;

    private final Statement statement;

    private ResultSet results;

    private final List<Row> rows = new ArrayList<>();

    public CassandraDatabaseQuery(final EntityContext context, final Statement statement, final CassandraDatabaseSession session)
    {
        if (null == context)
        {
            throw new NullArgumentException("context");
        }
        if (null == statement)
        {
            throw new NullArgumentException("statement");
        }
        if (null == session)
        {
            throw new NullArgumentException("session");
        }
        this.session = session;
        this.context = context;
        this.statement = statement;
    }

    @Override
    public T first() throws NormandraException
    {
        if (this.ensurResults().isExhausted())
        {
            if (this.rows.isEmpty())
            {
                return null;
            }
            final Row row = this.rows.get(0);
            return this.build(row);
        }
        else
        {
            final Row first = this.ensurResults().one();
            if (null == first)
            {
                return null;
            }
            this.rows.add(first);
            return this.build(first);
        }
    }

    @Override
    public T last() throws NormandraException
    {
        final List<T> list = this.list();
        if (null == list || list.isEmpty())
        {
            return null;
        }
        return list.get(list.size() - 1);
    }

    @Override
    public List<T> list() throws NormandraException
    {
        final int read = this.readAll();
        final List<T> elements = new ArrayList<>(Math.max(10, read));
        for (final Row row : this.rows)
        {
            final T element = this.build(row);
            if (element != null)
            {
                elements.add(element);
            }
        }
        return Collections.unmodifiableList(elements);
    }

    @Override
    public int size() throws NormandraException
    {
        this.readAll();
        return this.rows.size();
    }

    @Override
    public Collection<T> subset(final int offset, final int count) throws NormandraException
    {
        final List<T> elements = this.list();
        if (elements.isEmpty())
        {
            return Collections.emptyList();
        }
        return elements.subList(offset, offset + count);
    }

    @Override
    public Iterator<T> iterator()
    {
        return new Iterator<T>()
        {
            @Override
            public boolean hasNext()
            {
                return !ensurResults().isExhausted();
            }

            @Override
            public T next()
            {
                final Row row = ensurResults().one();
                if (null == row)
                {
                    return null;
                }
                rows.add(row);
                try
                {
                    return build(row);
                }
                catch (final Exception e)
                {
                    throw new IllegalStateException("Unable to get next entity [" + context + "] from row [" + row + "].", e);
                }
            }
        };
    }

    private int readAll()
    {
        int num = 0;
        for (final Row row : this.ensurResults())
        {
            this.rows.add(row);
            num++;
        }
        return num;
    }

    private T build(final Row row) throws NormandraException
    {
        if (null == row)
        {
            return null;
        }

        final TableMeta table = this.context.findTable(row.getColumnDefinitions().getTable(0));
        if (null == table)
        {
            return null;
        }

        final Map<ColumnMeta, Object> data;
        try
        {
            data = CassandraUtils.unpackValues(table, row);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to toEntity row values [" + row + "].", e);
        }
        if (null == data || data.isEmpty())
        {
            return null;
        }

        final EntityMeta entity = this.context.findEntity(data);
        if (null == entity)
        {
            return null;
        }
        return (T) new EntityBuilder(this.session, new CassandraDataFactory(this.session)).build(this.context, data);
    }

    private ResultSet ensurResults()
    {
        if (this.results != null)
        {
            return this.results;
        }
        this.results = this.session.executeSync(this.statement, DatabaseActivity.Type.SELECT);
        return this.results;
    }
}
