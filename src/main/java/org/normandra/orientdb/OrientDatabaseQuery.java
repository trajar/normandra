package org.normandra.orientdb;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.normandra.DatabaseQuery;
import org.normandra.NormandraException;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.util.EntityBuilder;
import org.normandra.util.LazyCollection;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * orientdb query api
 * <p>
 * User: bowen Date: 6/9/14
 */
public class OrientDatabaseQuery<T> implements DatabaseQuery<T>
{
    private final OrientDatabaseSession session;

    private final EntityContext context;

    private final OrientQueryActivity query;

    private LazyCollection<ODocument> lazy = null;

    public OrientDatabaseQuery(final OrientDatabaseSession session, final EntityContext context, final OrientQueryActivity query)
    {
        this.session = session;
        this.context = context;
        this.query = query;
    }

    @Override
    public T first() throws NormandraException
    {
        final Iterator<T> itr = this.iterator();
        while (itr.hasNext())
        {
            final T item = itr.next();
            if (item != null)
            {
                return item;
            }
        }
        return null;
    }

    @Override
    public T last() throws NormandraException
    {
        ODocument last = null;
        final Iterator<ODocument> itr = this.ensureResults().iterator();
        while (itr.hasNext())
        {
            final ODocument item = itr.next();
            if (item != null)
            {
                last = item;
            }
        }
        if (null == last)
        {
            return null;
        }
        return this.build(last);
    }

    @Override
    public List<T> list() throws NormandraException
    {
        final List<T> list = new ArrayList<>();
        final Iterator<T> itr = this.iterator();
        while (itr.hasNext())
        {
            final T item = itr.next();
            if (item != null)
            {
                list.add(item);
            }
        }
        return Collections.unmodifiableList(list);
    }

    @Override
    public int size() throws NormandraException
    {
        return this.list().size();
    }

    @Override
    public Collection<T> subset(int offset, int count) throws NormandraException
    {
        final Collection<ODocument> subset = this.ensureResults().subset(offset, count);
        if (null == subset || subset.isEmpty())
        {
            return Collections.emptyList();
        }
        final List<T> items = new ArrayList<>(subset.size());
        for (final ODocument doc : subset)
        {
            final T item = this.build(doc);
            if (item != null)
            {
                items.add(item);
            }
        }
        return Collections.unmodifiableList(items);
    }

    @Override
    public Iterator<T> iterator()
    {
        final Iterator<ODocument> itr = this.ensureResults().iterator();
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
                    throw new IllegalStateException("Unable to get next entity [" + context + "] from document [" + doc.getIdentity() + "].", e);
                }
            }
        };
    }

    private LazyCollection<ODocument> ensureResults()
    {
        if (this.lazy != null)
        {
            return this.lazy;
        }
        this.lazy = new LazyCollection<>(this.query.execute());
        return this.lazy;
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
        final T element = (T) new EntityBuilder(this.session, factory).build(this.context, datamap);
        if (null == element)
        {
            return null;
        }
        final Object key = this.context.getId().fromEntity(element);
        if (key instanceof Serializable)
        {
            final EntityMeta meta = this.context.findEntity(datamap);
            this.session.getCache().put(meta, (Serializable) key, element);
        }
        return element;
    }
}
