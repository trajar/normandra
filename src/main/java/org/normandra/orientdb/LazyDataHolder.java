package org.normandra.orientdb;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.normandra.NormandraException;
import org.normandra.data.DataHolder;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.SingleEntityContext;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a lazy-loaded orientdb data holder
 * <p/>
 * User: bowen
 * Date: 4/5/14
 */
public class LazyDataHolder implements DataHolder
{
    private final AtomicBoolean loaded = new AtomicBoolean(false);

    private final OrientDatabaseSession session;

    private final EntityMeta entity;

    private final ColumnMeta column;

    private final Object key;

    private ODocument document;


    public LazyDataHolder(final OrientDatabaseSession session, final EntityMeta meta, final ColumnMeta column, final Object key)
    {
        this.session = session;
        this.entity = meta;
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
            throw new IllegalStateException("Unable to query lazy loaded results from [" + this.entity + "] column [" + this.column + "].", e);
        }
    }


    @Override
    public Object get() throws NormandraException
    {
        final ODocument doc = this.ensureResults();
        if (null == doc)
        {
            return null;
        }
        try
        {
            return OrientUtils.unpackValue(new SingleEntityContext(this.entity), doc, this.column);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to unpack lazy loaded results for column [" + this.column + "] on entity [" + this.entity + "].", e);
        }
    }


    private ODocument ensureResults() throws NormandraException
    {
        if (this.loaded.get())
        {
            return this.document;
        }
        synchronized (this)
        {
            if (this.loaded.get())
            {
                return this.document;
            }
            try
            {
                this.document = this.session.findDocument(this.entity, this.key);
                this.loaded.getAndSet(true);
            }
            catch (final Exception e)
            {
                throw new NormandraException("Unable to get orientdb document by key [" + this.key + "].", e);
            }
        }
        return this.document;
    }
}
