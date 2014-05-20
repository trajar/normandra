package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.DatabaseQuery;
import org.normandra.DatabaseSession;
import org.normandra.NormandraException;
import org.normandra.cache.EntityCache;
import org.normandra.cache.MemoryCache;
import org.normandra.data.BasicDataHolder;
import org.normandra.data.ColumnAccessor;
import org.normandra.data.DataHolder;
import org.normandra.data.DataHolderFactory;
import org.normandra.generator.IdGenerator;
import org.normandra.log.DatabaseActivity;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.SingleEntityContext;
import org.normandra.meta.TableMeta;
import org.normandra.util.EntityBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * orient database session
 * <p/>
 * User: bowen
 * Date: 5/14/14
 */
public class OrientDatabaseSession implements DatabaseSession, DataHolderFactory
{
    private final EntityCache cache = new MemoryCache();

    private final List<DatabaseActivity> activities = new ArrayList<>();

    private final ODatabaseDocumentTx database;

    private final AtomicBoolean userTransaction = new AtomicBoolean(false);


    public OrientDatabaseSession(final ODatabaseDocumentTx db)
    {
        if (null == db)
        {
            throw new NullArgumentException("document database");
        }
        this.database = db;
    }


    @Override
    public void close()
    {
        this.database.close();
    }


    @Override
    public void clear()
    {
        this.cache.clear();
        this.activities.clear();
    }


    @Override
    public DatabaseQuery executeDynamciQuery(EntityContext meta, String query, Map<String, Object> parameters) throws NormandraException
    {
        return null;
    }


    @Override
    public void beginWork() throws NormandraException
    {
        if (this.database.getTransaction() != null && this.database.getTransaction().isActive())
        {
            throw new NormandraException("Transaction already active.");
        }
        this.database.begin();
        this.userTransaction.getAndSet(true);
    }


    @Override
    public void commitWork() throws NormandraException
    {
        this.database.commit();
        this.userTransaction.getAndSet(false);
    }


    @Override
    public void rollbackWork() throws NormandraException
    {
        this.database.rollback();
        this.userTransaction.getAndSet(false);
    }


    @Override
    public List<DatabaseActivity> listActivity()
    {
        return Collections.unmodifiableList(this.activities);
    }


    @Override
    public void save(final EntityMeta meta, final Object element) throws NormandraException
    {
        if (null == meta)
        {
            throw new NullArgumentException("entity metadata");
        }
        if (null == element)
        {
            throw new NullArgumentException("element");
        }

        final boolean commitWhenDone;
        if (this.userTransaction.get())
        {
            commitWhenDone = false;
        }
        else
        {
            commitWhenDone = true;
            this.database.begin();
        }

        try
        {
            final DatabaseActivity.Type operation;
            final long start = System.currentTimeMillis();
            final ODocument document;
            final ORID existing = this.findIdByElement(meta, element);
            if (existing != null)
            {
                document = this.findDocument(existing);
                operation = DatabaseActivity.Type.UPDATE;
            }
            else
            {
                final String schemaName = OrientUtils.schemaName(meta);
                document = this.database.newInstance(schemaName);
                operation = DatabaseActivity.Type.INSERT;
            }
            for (final Map.Entry<ColumnMeta, ColumnAccessor> entry : meta.getAccessors())
            {
                final ColumnMeta column = entry.getKey();
                final ColumnAccessor accessor = entry.getValue();
                final IdGenerator generator = meta.getGenerator(column);
                Object value = accessor.isEmpty(element) ? null : accessor.getValue(element);
                if (generator != null && accessor.isEmpty(element))
                {
                    final Object generated = generator.generate(meta);
                    final DataHolder data = new BasicDataHolder(generated);
                    accessor.setValue(element, data, this);
                    value = generated;
                }
                if (value != null)
                {
                    final String name = OrientUtils.propertyName(column);
                    final OType type = OrientUtils.columnType(column);
                    final Object packed = OrientUtils.packRaw(column, value);
                    document.field(name, packed, type);
                }
            }
            document.save();
            if (commitWhenDone)
            {
                this.database.commit();
            }
            final long end = System.currentTimeMillis();
            this.activities.add(new OrientUpdateActivity(operation, document.getIdentity(), new Date(), end - start));
        }
        catch (final Exception e)
        {
            if (commitWhenDone)
            {
                this.database.rollback();
            }
            throw new NormandraException("Unable to create/save new orientdb document.", e);
        }
    }


    @Override
    public void delete(final EntityMeta meta, final Object element) throws NormandraException
    {
        if (null == meta)
        {
            throw new NullArgumentException("entity metadata");
        }
        if (null == element)
        {
            throw new NullArgumentException("element");
        }

        final boolean commitWhenDone;
        if (this.userTransaction.get())
        {
            commitWhenDone = false;
        }
        else
        {
            commitWhenDone = true;
            this.database.begin();
        }

        try
        {
            final long start = System.currentTimeMillis();
            final ORID rid = this.findIdByElement(meta, element);
            if (null == rid)
            {
                throw new IllegalStateException("Unable to get orientdb record id from element [" + element + "].");
            }
            this.database.delete(rid);
            if (commitWhenDone)
            {
                this.database.commit();
            }
            final long end = System.currentTimeMillis();
            this.activities.add(new OrientUpdateActivity(DatabaseActivity.Type.DELETE, rid, new Date(), end - start));
        }
        catch (final Exception e)
        {
            this.database.rollback();
            throw new NormandraException("Unable to delete orientdb document.", e);
        }
    }


    @Override
    public DatabaseQuery executeNamedQuery(EntityContext meta, String name, Map<String, Object> parameters) throws NormandraException
    {
        return null;
    }


    @Override
    public DataHolder createStatic(Object value)
    {
        return new BasicDataHolder(value);
    }


    @Override
    public DataHolder createLazy(EntityMeta meta, TableMeta table, ColumnMeta column, Object key)
    {
        return new OrientLazyDataHolder(this, meta, column, key);
    }


    @Override
    public boolean exists(final EntityContext context, final Object key) throws NormandraException
    {
        if (null == context)
        {
            throw new NullArgumentException("entity context");
        }
        if (null == key)
        {
            throw new NullArgumentException("key");
        }

        try
        {
            for (final EntityMeta meta : context.getEntities())
            {
                if (this.findDocument(meta, key) != null)
                {
                    return true;
                }
            }
            return false;
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to delete orientdb document.", e);
        }
    }


    @Override
    public boolean exists(final EntityMeta meta, final Object key) throws NormandraException
    {
        if (null == meta)
        {
            throw new NullArgumentException("entity metadata");
        }
        if (null == key)
        {
            throw new NullArgumentException("key");
        }

        try
        {
            return this.findIdByKey(meta, key) != null;
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to find orientdb document by key [" + key + "].", e);
        }
    }


    @Override
    public Object get(final EntityContext context, final Object key) throws NormandraException
    {
        if (null == context)
        {
            throw new NullArgumentException("entity context");
        }
        if (null == key)
        {
            throw new NullArgumentException("key");
        }

        try
        {
            final ODocument document = this.findDocument(context, key);
            if (null == document)
            {
                return null;
            }

            final Map<ColumnMeta, Object> data = OrientUtils.unpackValues(context, document);
            if (data.isEmpty())
            {
                return null;
            }

            final EntityMeta meta = context.findEntity(data);
            if (null == meta)
            {
                return null;
            }

            final EntityBuilder builder = new EntityBuilder(this, this);
            return builder.build(context, data);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get orientdb document by key [" + key + "].", e);
        }
    }


    @Override
    public Object get(final EntityMeta meta, final Object key) throws NormandraException
    {
        if (null == meta)
        {
            throw new NullArgumentException("entity meta");
        }
        return this.get(new SingleEntityContext(meta), key);
    }


    @Override
    public List<Object> get(final EntityContext context, final Object... keys) throws NormandraException
    {
        if (null == context)
        {
            throw new NullArgumentException("entity meta");
        }
        if (null == keys || keys.length <= 0)
        {
            return Collections.emptyList();
        }

        if (context.getPrimaryKeys().size() > 1)
        {
            throw new NormandraException("Unable to query orientdb records with multiple primary keys - use a single / composite column.");
        }
        final ColumnMeta primary = context.getPrimaryKeys().iterator().next();
        boolean numeric = Number.class.isAssignableFrom(primary.getType());

        // query ids for each entity context
        final List<Object> list = new ArrayList<>(keys.length);
        for (final EntityMeta meta : context.getEntities())
        {
            // build query statement

            final StringBuilder text = new StringBuilder();
            text.append("select from ").append(OrientUtils.schemaName(meta));
            text.append(" where ").append(OrientUtils.propertyName(primary)).append(" in [");
            boolean first = true;
            for (final Object key : keys)
            {
                if (!first)
                {
                    text.append(",");
                }
                if (numeric)
                {
                    text.append(key);
                }
                else
                {
                    text.append("'").append(key).append("'");
                }
                first = false;
            }
            text.append("]");
            final OSQLSynchQuery query = new OSQLSynchQuery(text.toString(), keys.length);
            list.addAll(this.database.query(query));
        }
        return Collections.unmodifiableList(list);
    }


    @Override
    public List<Object> get(final EntityMeta meta, final Object... keys) throws NormandraException
    {
        return this.get(new SingleEntityContext(meta), keys);
    }


    protected final ODocument findDocument(final ORID rid)
    {
        if (null == rid)
        {
            return null;
        }
        final ODocument document = this.database.load(rid);
        if (null == document)
        {
            throw new IllegalStateException("Unable to load orientdb record id [" + rid + "].");
        }
        return document;
    }


    protected final ODocument findDocument(final EntityMeta meta, final Object key) throws NormandraException
    {
        final ORID rid = this.findIdByKey(meta, key);
        if (null == rid)
        {
            return null;
        }
        return this.findDocument(rid);
    }


    protected final ODocument findDocument(final EntityContext context, final Object key) throws NormandraException
    {
        if (null == context || null == key)
        {
            return null;
        }

        if (key instanceof ORID)
        {
            return this.findDocument((ORID) key);
        }

        for (final EntityMeta meta : context.getEntities())
        {
            final ORID rid = this.findIdByKey(meta, key);
            if (rid != null)
            {
                return this.findDocument(rid);
            }
        }
        return null;
    }


    protected final ORID findIdByKey(final EntityMeta meta, final Object key)
    {
        if (null == meta || null == key)
        {
            return null;
        }

        if (key instanceof ORID)
        {
            return (ORID) key;
        }

        final long start = System.currentTimeMillis();
        final String indexName = OrientUtils.keyIndex(meta);
        final String schemaName = OrientUtils.schemaName(meta);
        final OIndex keyIdx = this.database.getMetadata().getIndexManager().getClassIndex(schemaName, indexName);
        if (null == keyIdx)
        {
            throw new IllegalStateException("Unable to locate orientdb index [" + indexName + "].");
        }

        final OIdentifiable guid = (OIdentifiable) keyIdx.get(key);
        final long end = System.currentTimeMillis();
        this.activities.add(new OrientIndexActivity(DatabaseActivity.Type.SELECT, indexName, key, new Date(), end - start));
        if (null == guid)
        {
            return null;
        }
        return guid.getIdentity();
    }


    protected final ORID findIdByElement(final EntityMeta meta, final Object element) throws NormandraException
    {
        if (null == meta || null == element)
        {
            return null;
        }

        final Object key = meta.getId().fromEntity(element);
        if (null == key)
        {
            return null;
        }

        return this.findIdByKey(meta, key);
    }
}
