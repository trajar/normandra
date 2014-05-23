package org.normandra.orientdb;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
            DatabaseActivity.Type operation = DatabaseActivity.Type.INSERT;
            final long start = System.currentTimeMillis();
            final List<ODocument> documents = new LinkedList<>();
            for (final TableMeta table : meta)
            {
                final ODocument document;
                final ORID existing = this.findIdByElement(meta, table, element);
                if (existing != null)
                {
                    document = this.findDocument(existing);
                    operation = DatabaseActivity.Type.UPDATE;
                }
                else
                {
                    final String schemaName = meta.getName();
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
                        final String name = column.getName();
                        final OType type = OrientUtils.columnType(column);
                        final Object packed = OrientUtils.packRaw(column, value);
                        document.field(name, packed, type);
                    }
                }
                document.save();
                documents.add(document);
            }
            if (commitWhenDone)
            {
                this.database.commit();
            }

            final long end = System.currentTimeMillis();
            final List<ORID> rids = new ArrayList<>(documents.size());
            for (final ODocument document : documents)
            {
                rids.add(document.getIdentity());
            }
            this.activities.add(new OrientUpdateActivity(operation, rids, new Date(), end - start));

            this.cache.put(meta, element);
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
            final List<ORID> rids = new LinkedList<>();
            for (final TableMeta table : meta)
            {
                final ORID rid = this.findIdByElement(meta, table, element);
                if (rid != null)
                {
                    rids.add(rid);
                    this.database.delete(rid);
                }
            }
            if (commitWhenDone)
            {
                this.database.commit();
            }
            this.cache.remove(meta, element);
            final long end = System.currentTimeMillis();
            this.activities.add(new OrientUpdateActivity(DatabaseActivity.Type.DELETE, rids, new Date(), end - start));
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
        return new OrientLazyDataHolder(this, meta, table, column, key);
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
                for (final TableMeta table : meta)
                {
                    if (!table.isSecondary())
                    {
                        if (this.findDocument(meta, table, key) != null)
                        {
                            return true;
                        }
                    }
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
            for (final TableMeta table : meta)
            {
                if (!table.isSecondary())
                {
                    final ORID rid = this.findIdByKey(meta, table, key);
                    if (rid != null)
                    {
                        return true;
                    }
                }
            }
            return false;
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

        if (key instanceof Serializable)
        {
            // check cache
            for (final EntityMeta entity : context.getEntities())
            {
                final Object existing = this.cache.get(entity, (Serializable) key);
                if (existing != null)
                {
                    return entity.getType().cast(existing);
                }
            }
        }

        try
        {
            final Map<ColumnMeta, Object> data = new TreeMap<>();
            for (final TableMeta table : context.getPrimaryTables())
            {
                final ODocument document = this.findDocument(context, table, key);
                if (document != null)
                {
                    data.putAll(OrientUtils.unpackValues(context, document));
                }
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

        // query ids for each entity context
        final List<ORID> rids = new ArrayList<>(keys.length);
        for (final EntityMeta meta : context.getEntities())
        {
            for (final TableMeta table : new SingleEntityContext(meta).getPrimaryTables())
            {
                rids.addAll(this.findIdByKeys(meta, table, Arrays.asList(keys)));
            }
        }
        final Map<Object, Map<ColumnMeta, Object>> entityData = new HashMap<>(rids.size());
        for (final ORID rid : rids)
        {
            final ODocument document = this.findDocument(rid);
            if (document != null)
            {
                final Map<ColumnMeta, Object> data = OrientUtils.unpackValues(context, document);
                final EntityMeta entity = context.findEntity(data);
                if (entity != null)
                {
                    final Map<String, Object> keymap = new TreeMap<>();
                    for (final Map.Entry<ColumnMeta, Object> entry : data.entrySet())
                    {
                        keymap.put(entry.getKey().getName(), entry.getValue());
                    }
                    final Object key = entity.getId().toKey(keymap);
                    if (key != null)
                    {
                        Map<ColumnMeta, Object> datamap = entityData.get(key);
                        if (null == datamap)
                        {
                            datamap = new TreeMap<>();
                            entityData.put(key, datamap);
                        }
                        datamap.putAll(data);
                    }
                }
            }
        }
        if (entityData.isEmpty())
        {
            return Collections.emptyList();
        }
        final List<Object> result = new ArrayList<>(entityData.size());
        for (final Map.Entry<Object, Map<ColumnMeta, Object>> entry : entityData.entrySet())
        {
            final EntityBuilder builder = new EntityBuilder(this, this);
            final Object instance = builder.build(context, entry.getValue());
            if (instance != null)
            {
                result.add(instance);
            }
        }
        return Collections.unmodifiableList(result);
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


    protected final ODocument findDocument(final EntityMeta meta, final TableMeta table, final Object key) throws NormandraException
    {
        final ORID rid = this.findIdByKey(meta, table, key);
        if (null == rid)
        {
            return null;
        }
        return this.findDocument(rid);
    }


    protected final ODocument findDocument(final EntityContext context, final TableMeta table, final Object key) throws NormandraException
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
            final ORID rid = this.findIdByKey(meta, table, key);
            if (rid != null)
            {
                return this.findDocument(rid);
            }
        }
        return null;
    }


    protected final Collection<ORID> findIdByKeys(final EntityMeta meta, final TableMeta table, final Collection<Object> keys) throws NormandraException
    {
        if (null == meta || null == keys || keys.isEmpty())
        {
            return Collections.emptyList();
        }

        final List<Object> unknown = new ArrayList<>(keys.size());
        final List<ORID> result = new ArrayList<>(keys.size());
        for (final Object key : keys)
        {
            if (key instanceof ORID)
            {
                result.add((ORID) key);
            }
            else
            {
                unknown.add(key);
            }
        }

        final long start = System.currentTimeMillis();
        final String indexName = OrientUtils.keyIndex(meta);
        final String schemaName = meta.getName();
        final OIndex keyIdx = this.database.getMetadata().getIndexManager().getClassIndex(schemaName, indexName);
        if (null == keyIdx)
        {
            throw new IllegalStateException("Unable to locate orientdb index [" + indexName + "].");
        }

        final List<Object> query = new ArrayList<>(unknown.size());
        for (final Object key : unknown)
        {
            final Map<String, Object> map = meta.getId().fromKey(key);
            if (keys.isEmpty())
            {
                return null;
            }

            final List<Object> packed = new ArrayList<>(map.size());
            for (final ColumnMeta column : new SingleEntityContext(meta).getPrimaryKeys())
            {
                Object value = map.get(column.getName());
                if (null == value)
                {
                    value = map.get(column.getProperty());
                }
                if (value != null)
                {
                    packed.add(OrientUtils.packRaw(column, value));
                }
                else
                {
                    packed.add(null);
                }
            }
            if (packed.size() == 1)
            {
                query.add(packed.get(0));
            }
            else
            {
                query.add(new OCompositeKey(packed));
            }
        }

        if (query.isEmpty())
        {
            return Collections.unmodifiableList(result);
        }

        result.addAll(keyIdx.getValues(query, true));
        final long end = System.currentTimeMillis();
        this.activities.add(new OrientIndexActivity(DatabaseActivity.Type.SELECT, indexName, keys, new Date(), end - start));
        return Collections.unmodifiableList(result);
    }


    protected final ORID findIdByKey(final EntityMeta meta, final TableMeta table, final Object key) throws NormandraException
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
        final OIndex keyIdx = this.database.getMetadata().getIndexManager().getClassIndex(meta.getName(), indexName);
        if (null == keyIdx)
        {
            throw new IllegalStateException("Unable to locate orientdb index [" + indexName + "].");
        }

        final Map<String, Object> keys = meta.getId().fromKey(key);
        if (keys.isEmpty())
        {
            return null;
        }

        final OIdentifiable guid;
        final List<Object> packed = new ArrayList<>(keys.size());
        for (final ColumnMeta column : new SingleEntityContext(meta).getPrimaryKeys())
        {
            Object value = keys.get(column.getName());
            if (null == value)
            {
                value = keys.get(column.getProperty());
            }
            if (value != null)
            {
                packed.add(OrientUtils.packRaw(column, value));
            }
            else
            {
                packed.add(null);
            }
        }
        if (packed.size() == 1)
        {
            guid = (OIdentifiable) keyIdx.get(packed.get(0));
        }
        else
        {
            guid = (OIdentifiable) keyIdx.get(new OCompositeKey(packed));
        }

        final long end = System.currentTimeMillis();
        this.activities.add(new OrientIndexActivity(DatabaseActivity.Type.SELECT, indexName, Arrays.asList(key), new Date(), end - start));
        if (null == guid)
        {
            return null;
        }
        return guid.getIdentity();
    }


    protected final ORID findIdByElement(final EntityMeta meta, final TableMeta table, final Object element) throws NormandraException
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

        return this.findIdByKey(meta, table, key);
    }
}
