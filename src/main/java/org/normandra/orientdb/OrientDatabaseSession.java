package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.AbstractTransactional;
import org.normandra.DatabaseQuery;
import org.normandra.DatabaseSession;
import org.normandra.NormandraException;
import org.normandra.Transaction;
import org.normandra.cache.EntityCache;
import org.normandra.data.ColumnAccessor;
import org.normandra.log.DatabaseActivity;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.SingleEntityContext;
import org.normandra.meta.TableMeta;
import org.normandra.util.EntityBuilder;
import org.normandra.util.EntityPersistence;
import org.normandra.util.LazyCollection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * orient database session
 * <p>
 * User: bowen
 * Date: 5/14/14
 */
public class OrientDatabaseSession extends AbstractTransactional implements DatabaseSession
{
    private final EntityCache cache;

    private final List<DatabaseActivity> activities = new ArrayList<>();

    private final ODatabaseDocumentTx database;

    private final Map<String, OrientQuery> statementsByName;

    private final AtomicBoolean userTransaction = new AtomicBoolean(false);


    public OrientDatabaseSession(final ODatabaseDocumentTx db, final Map<String, OrientQuery> statements, final EntityCache cache)
    {
        if (null == db)
        {
            throw new NullArgumentException("document database");
        }
        if (null == cache)
        {
            throw new NullArgumentException("cache");
        }
        this.cache = cache;
        this.database = db;
        this.database.setMVCC(true);
        this.statementsByName = new TreeMap<>(statements);
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
    public boolean pendingWork()
    {
        return this.userTransaction.get();
    }


    @Override
    public void beginWork() throws NormandraException
    {
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

        try (final Transaction tx = this.beginTransaction())
        {
            final long start = System.currentTimeMillis();
            final DatabaseActivity.Type operation = DatabaseActivity.Type.UPDATE;
            final OrientDataHandler handler = new OrientDataHandler(this);
            new EntityPersistence(this).save(meta, element, handler);
            tx.success();
            final long end = System.currentTimeMillis();

            final Collection<ODocument> documents = handler.getDocuments();
            final List<ORID> rids = new ArrayList<>(documents.size());
            for (final ODocument document : documents)
            {
                rids.add(document.getIdentity());
            }
            this.activities.add(new OrientUpdateActivity(operation, rids, new Date(), end - start));

            final Object key = meta.getId().fromEntity(element);
            this.cache.put(meta, key, element);
        }
        catch (final Exception e)
        {
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

        try (final Transaction tx = this.beginTransaction())
        {
            final EntityContext context = new SingleEntityContext(meta);
            final long start = System.currentTimeMillis();
            final List<OIdentifiable> rids = new LinkedList<>();
            for (final TableMeta table : meta)
            {
                final Map<String, Object> datamap = new TreeMap<>();
                for (final ColumnMeta column : table.getPrimaryKeys())
                {
                    final ColumnAccessor accessor = meta.getAccessor(column);
                    final Object value = accessor != null ? accessor.getValue(element, this) : null;
                    if (value != null)
                    {
                        datamap.put(column.getName(), value);
                    }
                }
                final OIdentifiable rid = this.findIdByMap(context, table, datamap);
                if (rid != null)
                {
                    rids.add(rid);
                    this.database.delete(rid.getIdentity());
                }
            }
            tx.success();

            final Object key = meta.getId().fromEntity(element);
            this.cache.remove(meta, key);

            final long end = System.currentTimeMillis();
            this.activities.add(new OrientUpdateActivity(DatabaseActivity.Type.DELETE, rids, new Date(), end - start));
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to delete orientdb document.", e);
        }
    }


    @Override
    public DatabaseQuery executeNamedQuery(final EntityContext meta, final String name, final Map<String, Object> params) throws NormandraException
    {
        final OrientQuery query = this.statementsByName.get(name);
        if (null == query)
        {
            return null;
        }
        final OrientQueryActivity activity = new OrientQueryActivity(this.database, query.getQuery(), params);
        this.activities.add(activity);
        return new OrientDatabaseQuery(this, meta, activity);
    }


    @Override
    public DatabaseQuery executeDynamciQuery(final EntityContext meta, final String query, final Map<String, Object> params) throws NormandraException
    {
        final OrientQueryActivity activity = new OrientQueryActivity(this.database, query, params);
        this.activities.add(activity);
        return new OrientDatabaseQuery(this, meta, activity);
    }


    protected Collection<ODocument> query(final String query, final Collection<?> args)
    {
        final OrientQueryActivity activity = new OrientQueryActivity(this.database, query, args);
        this.activities.add(activity);
        return new LazyCollection<>(activity.execute());
    }


    protected Collection<ODocument> query(final String query, final Map<String, Object> args)
    {
        final OrientQueryActivity activity = new OrientQueryActivity(this.database, query, args);
        this.activities.add(activity);
        return new LazyCollection<>(activity.execute());
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
                    if (!table.isJoinTable())
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
            final OIdentifiable rid = this.findIdByKey(new SingleEntityContext(meta), key);
            return rid != null;
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

        final Object existing = this.cache.get(context, key, Object.class);
        if (existing != null)
        {
            return existing;
        }

        try
        {
            final Map<ColumnMeta, Object> data = new TreeMap<>();
            for (final TableMeta table : context.getPrimaryTables())
            {
                final OIdentifiable rid = this.findDocument(context, table, key);
                final ODocument document = this.findDocument(rid);
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

            final EntityBuilder builder = new EntityBuilder(this, new OrientDataFactory(this));
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

        // query context
        final Set<Object> keyset = new HashSet<>(Arrays.asList(keys));
        final Map<Object, Object> cached = this.cache.find(context, Arrays.asList(keys), Object.class);
        keyset.removeAll(cached.keySet());
        if (keyset.isEmpty())
        {
            return Collections.unmodifiableList(new ArrayList<>(cached.values()));
        }

        // query ids for each entity context
        final Set<OIdentifiable> rids = new TreeSet<>();
        for (final TableMeta table : context.getPrimaryTables())
        {
            rids.addAll(this.findIdByKeys(context, table, keyset));
        }
        final Map<Object, Map<ColumnMeta, Object>> entityData = new HashMap<>(rids.size());
        for (final OIdentifiable rid : rids)
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

        // build entities
        final List<Object> result = new ArrayList<>(cached.values());
        for (final Map.Entry<Object, Map<ColumnMeta, Object>> entry : entityData.entrySet())
        {
            final Map<ColumnMeta, Object> data = entry.getValue();
            final EntityBuilder builder = new EntityBuilder(this, new OrientDataFactory(this));
            final Object instance = builder.build(context, data);
            if (instance != null)
            {
                final Object key = context.findEntity(data);
                this.cache.put(context, key, instance);
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


    protected final ODatabaseDocumentTx getDatabase()
    {
        return this.database;
    }


    public final ODocument findDocument(final OIdentifiable item)
    {
        if (null == item)
        {
            return null;
        }
        if (item instanceof ODocument)
        {
            return (ODocument) item;
        }
        final ORID rid = item.getIdentity();
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


    public final ODocument findDocument(final EntityMeta meta, final TableMeta table, final Object key)
    {
        final OIdentifiable rid = this.findIdByKey(new SingleEntityContext(meta), table, key);
        if (null == rid)
        {
            return null;
        }
        return this.findDocument(rid);
    }


    public final ODocument findDocument(final EntityContext context, final TableMeta table, final Object key)
    {
        if (null == context || null == key)
        {
            return null;
        }

        if (key instanceof OIdentifiable)
        {
            return this.findDocument((OIdentifiable) key);
        }

        final OIdentifiable rid = this.findIdByKey(context, table, key);
        if (rid != null)
        {
            return this.findDocument(rid);
        }
        else
        {
            return null;
        }
    }


    public final Collection<OIdentifiable> findIdByKeys(final EntityContext meta, final TableMeta table, final Collection<Object> keys)
    {
        if (null == meta || null == table || null == keys || keys.isEmpty())
        {
            return Collections.emptyList();
        }

        final List<Object> unknown = new ArrayList<>(keys.size());
        final List<OIdentifiable> result = new ArrayList<>(keys.size());
        for (final Object key : keys)
        {
            if (key instanceof OIdentifiable)
            {
                result.add((OIdentifiable) key);
            }
            else
            {
                unknown.add(key);
            }
        }
        if (unknown.isEmpty())
        {
            return Collections.unmodifiableList(result);
        }

        final long start = System.currentTimeMillis();
        final String indexName = OrientUtils.keyIndex(table);
        final String schemaName = table.getName();
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
            for (final ColumnMeta column : table.getPrimaryKeys())
            {
                Object value = map.get(column.getName());
                if (null == value)
                {
                    value = map.get(column.getProperty());
                }
                if (value != null)
                {
                    packed.add(OrientUtils.packValue(column, value));
                }
                else
                {
                    packed.add(null);
                }
            }
            if (table.getPrimaryKeys().size() == 1)
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

        for (final Object key : query)
        {
            final OIdentifiable rid = (ORID) keyIdx.get(key);
            final ORecord record = rid != null ? this.database.load(rid.getIdentity()) : null;
            if (record != null)
            {
                result.add(record);
            }
        }
        final long end = System.currentTimeMillis();
        this.activities.add(new OrientIndexActivity(DatabaseActivity.Type.SELECT, indexName, query, new Date(), end - start));
        return Collections.unmodifiableList(result);
    }


    protected final OIdentifiable findIdByMap(final EntityContext meta, final TableMeta table, final Map<String, Object> map)
    {
        if (null == meta || null == table || null == map || map.isEmpty())
        {
            return null;
        }

        final long start = System.currentTimeMillis();
        final String indexName = OrientUtils.keyIndex(table);
        final String schemaName = table.getName();
        final OIndex keyIdx = this.database.getMetadata().getIndexManager().getClassIndex(schemaName, indexName);
        if (null == keyIdx)
        {
            throw new IllegalStateException("Unable to locate orientdb index [" + indexName + "].");
        }

        final OIdentifiable guid;
        final List<Object> packed = new ArrayList<>(map.size());
        for (final ColumnMeta column : table.getPrimaryKeys())
        {
            Object value = map.get(column.getName());
            if (null == value)
            {
                value = map.get(column.getProperty());
            }
            if (value != null)
            {
                packed.add(OrientUtils.packValue(column, value));
            }
            else
            {
                packed.add(null);
            }
        }
        if (packed.isEmpty())
        {
            return null;
        }
        else if (table.getPrimaryKeys().size() == 1)
        {
            guid = (OIdentifiable) keyIdx.get(packed.get(0));
        }
        else
        {
            guid = (OIdentifiable) keyIdx.get(new OCompositeKey(packed));
        }

        final long end = System.currentTimeMillis();
        this.activities.add(new OrientIndexActivity(DatabaseActivity.Type.SELECT, indexName, Arrays.asList(guid), new Date(), end - start));
        return guid;
    }


    public final OIdentifiable findIdByKey(final EntityContext meta, final TableMeta table, final Object key)
    {
        if (null == meta || null == table || null == key)
        {
            return null;
        }

        if (key instanceof OIdentifiable)
        {
            return (OIdentifiable) key;
        }

        final Map<String, Object> keymap = meta.getId().fromKey(key);
        if (keymap.isEmpty())
        {
            return null;
        }

        return this.findIdByMap(meta, table, keymap);
    }


    public final OIdentifiable findIdByKey(final EntityContext meta, final Object key)
    {
        if (null == meta || null == key)
        {
            return null;
        }

        if (key instanceof OIdentifiable)
        {
            return (OIdentifiable) key;
        }

        final Map<String, Object> keymap = meta.getId().fromKey(key);
        if (keymap.isEmpty())
        {
            return null;
        }

        for (final TableMeta table : meta.getPrimaryTables())
        {
            final OIdentifiable rid = this.findIdByMap(meta, table, keymap);
            if (rid != null)
            {
                return rid;
            }
        }
        return null;
    }


    final <T> T build(final EntityContext context, final ODocument document) throws NormandraException
    {
        if (null == document)
        {
            return null;
        }

        final Map<ColumnMeta, Object> datamap = OrientUtils.unpackValues(context, document);
        if (null == datamap || datamap.isEmpty())
        {
            return null;
        }

        final OrientDataFactory factory = new OrientDataFactory(this);
        final T element = (T) new EntityBuilder(this, factory).build(context, datamap);
        if (null == element)
        {
            return null;
        }
        final Object key = context.getId().fromEntity(element);
        final EntityMeta meta = context.findEntity(datamap);
        this.cache.put(meta, key, element);
        return element;
    }
}
