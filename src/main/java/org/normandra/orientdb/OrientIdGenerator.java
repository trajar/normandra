package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.normandra.NormandraException;
import org.normandra.generator.IdGenerator;
import org.normandra.meta.EntityMeta;

/**
 * a sequenced id generator
 * <p>
 * User: bowen
 * Date: 1/26/14
 */
public class OrientIdGenerator implements IdGenerator
{
    private static final Object lock = new Object();

    private final String tableName;

    private final String indexName;

    private final String keyColumn;

    private final String valueColumn;

    private final String keyValue;

    private final OrientDatabase database;

    protected OrientIdGenerator(final String table, final String index, final String keyCol, final String valueCol, final String key, final OrientDatabase database)
    {
        this.tableName = table;
        this.indexName = index;
        this.keyColumn = keyCol;
        this.valueColumn = valueCol;
        this.keyValue = key;
        this.database = database;
    }

    @Override
    public Long generate(final EntityMeta entity) throws NormandraException
    {
        if (null == entity)
        {
            return null;
        }

        synchronized (lock)
        {
            try (final ODatabaseDocumentTx dbtx = this.database.createDatabase())
            {
                // try to update counter a fixed number of times
                dbtx.begin();
                for (int i = 0; i < 10; i++)
                {
                    final Long value = this.incrementCounter(dbtx);
                    if (value != null)
                    {
                        dbtx.commit();
                        return value;
                    }
                }
                dbtx.rollback();
            }
            catch (final Exception e)
            {
                throw new NormandraException("Unable to increment counter id [" + this.keyValue + "] from table [" + this.tableName + "].", e);
            }
        }

        throw new NormandraException("Unable to generate counter id.");
    }

    private Long incrementCounter(final ODatabaseDocumentTx dbtx)
    {
        ODocument document = this.findDocument(dbtx);
        if (null == document)
        {
            document = dbtx.newInstance(this.tableName);
            document.field(this.keyColumn, this.keyValue);
        }

        final Long current = document.field(this.valueColumn);
        final Long next;
        if (null == current)
        {
            next = 1L;
        }
        else
        {
            next = current.longValue() + 1;
        }
        document.field(this.valueColumn, next);
        document.save();
        document.detach();
        return next;
    }

    private ODocument findDocument(final ODatabaseDocumentTx dbtx)
    {
        final OIndex keyIdx = dbtx.getMetadata().getIndexManager().getClassIndex(this.tableName, this.indexName);
        if (null == keyIdx)
        {
            throw new IllegalStateException("Unable to locate orientdb index [" + this.indexName + "].");
        }

        final OIdentifiable guid = (OIdentifiable) keyIdx.get(this.keyValue);
        if (null == guid)
        {
            return null;
        }

        final ORID rid = guid.getIdentity();
        if (null == rid)
        {
            return null;
        }

        final ODocument document = dbtx.load(rid);
        if (null == document)
        {
            throw new IllegalStateException("Unable to load orientdb record id [" + rid + "].");
        }
        return document;
    }
}
