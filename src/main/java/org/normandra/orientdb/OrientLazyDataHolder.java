package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.normandra.NormandraException;
import org.normandra.data.DataHolder;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.TableMeta;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a lazy-loaded orientdb data holder based on key/index lookup
 * <p>
 * User: bowen
 * Date: 4/5/14
 */
public class OrientLazyDataHolder implements DataHolder
{
    private final AtomicBoolean loaded = new AtomicBoolean(false);

    private final OrientDatabaseSession session;

    private final EntityContext entity;

    private final TableMeta table;

    private final ColumnMeta column;

    private final Map<String, Object> key;

    private ODocument document;


    public OrientLazyDataHolder(final OrientDatabaseSession session, final EntityContext meta, final TableMeta table, final ColumnMeta column, final Map<String, Object> keys)
    {
        this.session = session;
        this.entity = meta;
        this.table = table;
        this.column = column;
        this.key = new TreeMap<>(keys);
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
            return OrientUtils.unpackValue(doc, this.column);
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
                final OIdentifiable rid = this.session.findIdByMap(this.entity, this.table, this.key);
                if (rid != null)
                {
                    this.document = this.session.findDocument(rid);
                }
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
