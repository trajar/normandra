package org.normandra.orientdb;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.normandra.DatabaseQuery;
import org.normandra.NormandraException;
import org.normandra.meta.EntityContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * orientdb query api
 * <p>
 * Date: 6/9/14
 */
public class OrientDatabaseQuery<T> implements DatabaseQuery<T>
{
    private final OrientDatabaseSession session;

    private final EntityContext context;

    private final OrientNonBlockingDocumentQuery query;

    private T firstItem = null;

    public OrientDatabaseQuery(final OrientDatabaseSession session, final EntityContext context, final OrientNonBlockingDocumentQuery query)
    {
        this.session = session;
        this.context = context;
        this.query = query;
    }

    @Override
    public T first() throws NormandraException
    {
        if (this.firstItem != null)
        {
            return this.firstItem;
        }

        for (final T item : this)
        {
            if (item != null)
            {
                this.firstItem = item;
                return item;
            }
        }

        return null;
    }

    @Override
    public List<T> list() throws NormandraException
    {
        final List<T> list = new ArrayList<>();
        for (final T item : this)
        {
            if (item != null)
            {
                list.add(item);
            }
        }
        return Collections.unmodifiableList(list);
    }

    @Override
    public boolean empty() throws NormandraException
    {
        return this.first() != null;
    }

    @Override
    public Iterator<T> iterator()
    {
        final Iterator<ODocument> itr = this.query.iterator();
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
                    return session.build(context, doc);
                }
                catch (final Exception e)
                {
                    throw new IllegalStateException("Unable to get next entity [" + context + "] from document [" + doc.getIdentity() + "].", e);
                }
            }
        };
    }
}
