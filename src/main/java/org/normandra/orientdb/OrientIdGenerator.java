package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.normandra.EntitySession;
import org.normandra.NormandraException;
import org.normandra.generator.IdGenerator;
import org.normandra.meta.EntityMeta;

/**
 * a sequenced id generator
 * <p>
 * <p>
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
    public Long generate(final EntitySession session, final EntityMeta entity) throws NormandraException
    {
        if (null == session)
        {
            return null;
        }
        if (null == entity)
        {
            return null;
        }

        synchronized (lock)
        {
            try
            {
                if (session instanceof OrientDatabaseSession)
                {
                    final ODatabaseDocumentTx db = ((OrientDatabaseSession) session).getDatabase();
                    return this.generateWithDatabase(db, entity);
                }
                else
                {
                    try (final ODatabaseDocumentTx db = this.database.createDatabase())
                    {
                        return this.generateWithDatabase(db, entity);
                    }
                }
            }
            catch (final Exception e)
            {
                throw new NormandraException("Unable to increment counter id [" + this.keyValue + "] from table [" + this.tableName + "].", e);
            }
        }
    }

    private Long generateWithDatabase(final ODatabaseDocument db, final EntityMeta entity)
    {
        // try to update counter a fixed number of times
        db.begin();
        for (int i = 0; i < 10; i++)
        {
            final Long value = this.incrementCounter(db);
            if (value != null)
            {
                db.commit();
                return value;
            }
        }
        db.rollback();

        throw new IllegalStateException("Unable to generate counter id.");
    }

    private Long incrementCounter(final ODatabaseDocument dbtx)
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

    private ODocument findDocument(final ODatabaseDocument dbtx)
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
