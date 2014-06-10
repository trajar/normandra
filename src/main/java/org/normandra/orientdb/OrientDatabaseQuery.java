package org.normandra.orientdb;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.normandra.DatabaseQuery;
import org.normandra.NormandraException;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.util.EntityBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * orientdb query api
 * <p/>
 * User: bowen
 * Date: 6/9/14
 */
public class OrientDatabaseQuery<T> implements DatabaseQuery<T>
{
    private final OrientDatabaseSession session;

    private final EntityContext context;

    private final OrientQueryActivity query;


    public OrientDatabaseQuery(final OrientDatabaseSession session, final EntityContext context, final OrientQueryActivity query)
    {
        this.session = session;
        this.context = context;
        this.query = query;
    }


    @Override
    public T first() throws NormandraException
    {
        final Collection<ODocument> list = this.ensurResults();
        if (list.isEmpty())
        {
            return null;
        }
        return this.build(list.iterator().next());
    }


    @Override
    public T last() throws NormandraException
    {
        final List<ODocument> list = this.ensurResults();
        if (list.isEmpty())
        {
            return null;
        }
        final ODocument last = list.get(list.size() - 1);
        if (null == last)
        {
            return null;
        }
        return this.build(last);
    }


    @Override
    public List<T> list() throws NormandraException
    {
        final List<ODocument> list = this.ensurResults();
        if (list.isEmpty())
        {
            return Collections.emptyList();
        }
        final List<T> result = new ArrayList<>(list.size());
        final Iterator<T> itr = this.iterator();
        while (itr.hasNext())
        {
            final T item = itr.next();
            if (item != null)
            {
                result.add(item);
            }
        }
        return Collections.unmodifiableList(result);
    }


    @Override
    public int size() throws NormandraException
    {
        return this.ensurResults().size();
    }


    @Override
    public Collection<T> subset(int offset, int count) throws NormandraException
    {
        final List<T> items = this.list();
        if (items.isEmpty())
        {
            return Collections.emptyList();
        }
        return items.subList(offset, offset + count);
    }


    @Override
    public Iterator<T> iterator()
    {
        final List<ODocument> documents = this.ensurResults();
        final Iterator<ODocument> itr = documents.iterator();
        return new Iterator<T>()
        {
            @Override
            public boolean hasNext()
            {
                return itr.hasNext();
            }


            @Override
            public T next()
            {
                final ODocument doc = itr.next();
                if (null == doc)
                {
                    return null;
                }
                try
                {
                    return build(doc);
                }
                catch (final Exception e)
                {
                    throw new IllegalStateException("Unable to get next entity [" + context + "] from document [" + doc + "].", e);
                }
            }
        };
    }


    private T build(final ODocument document) throws NormandraException
    {
        if (null == document)
        {
            return null;
        }

        final Map<ColumnMeta, Object> datamap = OrientUtils.unpackValues(this.context, document);
        if (null == datamap || datamap.isEmpty())
        {
            return null;
        }

        final OrientDataFactory factory = new OrientDataFactory(this.session);
        return (T) new EntityBuilder(this.session, factory).build(this.context, datamap);
    }


    synchronized private List<ODocument> ensurResults()
    {
        if (this.query.isFinished())
        {
            return this.query.getResults();
        }
        else
        {
            return this.query.execute();
        }
    }
}
