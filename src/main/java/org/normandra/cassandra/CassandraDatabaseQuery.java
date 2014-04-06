package org.normandra.cassandra;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.DatabaseQuery;
import org.normandra.NormandraException;
import org.normandra.log.DatabaseActivity;
import org.normandra.meta.EntityContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * cassandra database query
 * <p>
 * User: bowen
 * Date: 4/5/14
 */
public class CassandraDatabaseQuery<T> implements DatabaseQuery<T>
{
    private final CassandraDatabaseSession session;

    private final EntityContext context;

    private final RegularStatement statement;

    private ResultSet results;

    private final List<Row> rows = new ArrayList<>();


    public CassandraDatabaseQuery(final EntityContext context, final RegularStatement statement, final CassandraDatabaseSession session)
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
            return (T) new CassandraEntityBuilder(this.session).build(this.context, row);
        }
        else
        {
            final Row first = this.ensurResults().one();
            if (null == first)
            {
                return null;
            }
            this.rows.add(first);
            return (T) new CassandraEntityBuilder(this.session).build(this.context, first);
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
        this.readAll();
        final List<T> elements = new ArrayList<>(this.rows.size());
        for (final Row row : this.rows)
        {
            final T element = (T) new CassandraEntityBuilder(this.session).build(this.context, row);
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
                    return (T) new CassandraEntityBuilder(session).build(context, row);
                }
                catch (final Exception e)
                {
                    throw new IllegalStateException("Unable to get next entity [" + context + "] from row [" + row + "].", e);
                }
            }
        };
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
}