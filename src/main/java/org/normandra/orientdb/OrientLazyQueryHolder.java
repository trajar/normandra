package org.normandra.orientdb;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.normandra.NormandraException;
import org.normandra.data.DataHolder;
import org.normandra.meta.EntityContext;
import org.normandra.meta.TableMeta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a lazy-loaded orientdb data holder based on query string
 * <p/>
 * User: bowen
 * Date: 4/5/14
 */
public class OrientLazyQueryHolder implements DataHolder
{
    private final AtomicBoolean loaded = new AtomicBoolean(false);

    private final OrientDatabaseSession session;

    private final EntityContext entity;

    private final TableMeta table;

    private final boolean collection;

    private final String query;

    private final List<Object> parameters;

    private final OrientDocumentHandler handler;

    private final List<ODocument> documents = new ArrayList<>();


    public OrientLazyQueryHolder(final OrientDatabaseSession session, final EntityContext meta, final TableMeta table, final boolean collection, final String query, final List<Object> params, final OrientDocumentHandler handler)
    {
        this.session = session;
        this.entity = meta;
        this.table = table;
        this.collection = collection;
        this.query = query;
        this.parameters = new ArrayList<>(params);
        this.handler = handler;
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
            throw new IllegalStateException("Unable to query lazy loaded results from [" + this.entity + "] table [" + this.table + "].", e);
        }
    }


    @Override
    public Object get() throws NormandraException
    {
        final Collection<ODocument> results = this.ensureResults();
        if (null == results || results.isEmpty())
        {
            return null;
        }
        try
        {
            if (this.collection)
            {
                final List<Object> items = new ArrayList<>(results.size());
                for (final ODocument doc : results)
                {
                    final Object item = this.handler.convert(doc);
                    if (item != null)
                    {
                        items.add(item);
                    }
                }
                return Collections.unmodifiableCollection(items);
            }
            else
            {
                return this.handler.convert(results.iterator().next());
            }
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to unpack lazy loaded results for table [" + this.table + "] on entity [" + this.entity + "].", e);
        }
    }


    private List<ODocument> ensureResults() throws NormandraException
    {
        if (this.loaded.get())
        {
            return Collections.unmodifiableList(this.documents);
        }
        synchronized (this)
        {
            if (this.loaded.get())
            {
                return Collections.unmodifiableList(this.documents);
            }
            try
            {
                final List<ODocument> results = this.session.query(this.query, this.parameters);
                if (null != results && !results.isEmpty())
                {
                    this.documents.addAll(results);
                }
                this.loaded.getAndSet(true);
            }
            catch (final Exception e)
            {
                throw new NormandraException("Unable to get orientdb document by query [" + this.query + "].", e);
            }
        }
        return Collections.unmodifiableList(this.documents);
    }
}
