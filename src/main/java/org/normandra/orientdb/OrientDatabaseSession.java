package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.tx.OTransaction;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.AbstractTransactional;
import org.normandra.DatabaseQuery;
import org.normandra.DatabaseSession;
import org.normandra.NormandraException;
import org.normandra.Transaction;
import org.normandra.cache.EntityCache;
import org.normandra.data.ColumnAccessor;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.SingleEntityContext;
import org.normandra.meta.TableMeta;
import org.normandra.util.EntityBuilder;
import org.normandra.util.EntityPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * orient database session
 * <p>
 * Date: 5/14/14
 */
public class OrientDatabaseSession extends AbstractTransactional implements DatabaseSession
{
    private static final Logger logger = LoggerFactory.getLogger(OrientDatabaseSession.class);

    private final EntityCache cache;

    private final ODatabaseDocumentTx database;

    private final Map<String, OrientQuery> statementsByName;

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
    }

    @Override
    public boolean pendingWork()
    {
        final OTransaction tx = this.database.getTransaction();
        if (null == tx)
        {
            logger.debug("Unable to locate database transaction.");
            return false;
        }
        else
        {
            return tx.isActive();
        }
    }

    @Override
    public void beginWork() throws NormandraException
    {
        if (logger.isDebugEnabled())
        {
            if (this.pendingWork())
            {
                logger.debug("Beginning transaction, but already in pending-work state.");
            }
        }
        this.database.begin();
    }

    @Override
    public void commitWork() throws NormandraException
    {
        if (logger.isDebugEnabled())
        {
            if (!this.pendingWork())
            {
                logger.debug("Committing transaction, but not in pending-work state.");
            }
        }
        this.database.commit();
    }

    @Override
    public void rollbackWork() throws NormandraException
    {
        if (logger.isDebugEnabled())
        {
            if (!this.pendingWork())
            {
                logger.debug("Rolling back transaction, but not in pending-work state.");
            }
        }
        this.database.rollback();
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
            final OrientDataHandler handler = new OrientDataHandler(this);
            new EntityPersistence(this).save(meta, element, handler);

            tx.success();

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
                final OIdentifiable rid = this.findIdByMap(table, datamap);
                if (rid != null)
                {
                    this.database.delete(rid.getIdentity());
                }
            }

            tx.success();

            final Object key = meta.getId().fromEntity(element);
            this.cache.remove(meta, key);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to delete orientdb document.", e);
        }
    }

    @Override
    public DatabaseQuery executeQuery(final EntityContext meta, final String queryOrName, final Map<String, Object> params) throws NormandraException
    {
        final OrientQuery query = this.statementsByName.get(queryOrName);
        if (query != null)
        {
            return this.executeNamedQuery(meta, query, params);
        }
        else
        {
            return this.executeDynamicQuery(meta, queryOrName, params);
        }
    }

    @Override
    public Object scalarQuery(final String queryOrName) throws NormandraException
    {
        try
        {
            final OrientQuery query = this.statementsByName.get(queryOrName);
            final OrientNonBlockingDocumentQuery synchronizedQuery;
            if (query != null)
            {
                synchronizedQuery = new OrientNonBlockingDocumentQuery(this.database, query.getQuery(), Collections.emptyList());
            }
            else
            {
                synchronizedQuery = new OrientNonBlockingDocumentQuery(this.database, queryOrName, Collections.emptyList());
            }
            final Iterator<ODocument> itr = synchronizedQuery.execute();
            while (itr.hasNext())
            {
                final ODocument document = itr.next();
                if (document != null)
                {
                    final Object[] values = document.fieldValues();
                    if (values != null && values.length > 0)
                    {
                        return values[0];
                    }
                }
            }
            return null;
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to execute scalar query.", e);
        }
    }

    private DatabaseQuery executeNamedQuery(final EntityContext meta, final OrientQuery query, final Map<String, Object> params) throws NormandraException
    {
        final OrientNonBlockingDocumentQuery activity = new OrientNonBlockingDocumentQuery(this.database, query.getQuery(), params);
        return new OrientDatabaseQuery(this, meta, activity);
    }

    private DatabaseQuery executeDynamicQuery(final EntityContext meta, final String query, final Map<String, Object> params) throws NormandraException
    {
        final OrientNonBlockingDocumentQuery activity = new OrientNonBlockingDocumentQuery(this.database, query, params);
        return new OrientDatabaseQuery(this, meta, activity);
    }

    protected final OrientNonBlockingDocumentQuery query(final String query)
    {
        return new OrientNonBlockingDocumentQuery(this.database, query, Collections.emptyList());
    }

    protected final OrientNonBlockingDocumentQuery query(final String query, final Collection<?> args)
    {
        return new OrientNonBlockingDocumentQuery(this.database, query, args);
    }

    protected final OrientNonBlockingDocumentQuery query(final String query, final Map<String, Object> args)
    {
        return new OrientNonBlockingDocumentQuery(this.database, query, args);
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
            return this.findIdByKey(context, key) != null;
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

        return this.exists(new SingleEntityContext(meta), key);
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
            final OIdentifiable rid = this.findIdByKey(context, key);
            final ODocument document = this.findDocument(rid);
            if (document != null)
            {
                data.putAll(OrientUtils.unpackValues(context, document));
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
        final Map<Object, Map<ColumnMeta, Object>> entityData = new HashMap<>();
        for (final OIdentifiable rid : this.findIdByKeys(context, keyset))
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

    public OIdentifiable findIdByMap(final TableMeta table, final Map<String, Object> keys)
    {
        final Set<ColumnMeta> sortedColumns = new TreeSet<>();
        keys.keySet().stream().map(table::getColumn).filter((x) -> x != null).forEach(sortedColumns::add);
        final Collection<Object> parameters = new ArrayList<>(sortedColumns.size());
        parameters.addAll(sortedColumns.stream().map(column -> OrientUtils.packValue(column, keys.get(column.getName()))).collect(Collectors.toList()));

        if (parameters.isEmpty())
        {
            return null;
        }

        if (parameters.size() == 1)
        {
            final String indexName = OrientUtils.keyIndex(table);
            final String schemaName = table.getName();
            final OIndex keyIdx = this.database.getMetadata().getIndexManager().getClassIndex(schemaName, indexName);
            if (keyIdx != null)
            {
                final Object packedKey = parameters.iterator().next();
                final Object value = keyIdx.get(packedKey);
                if (value instanceof OIdentifiable)
                {
                    return this.fixIdentifiable((OIdentifiable) value);
                }
                else
                {
                    return null;
                }
            }
        }

        final StringBuilder query = new StringBuilder()
            .append("SELECT FROM INDEX:").append(OrientUtils.keyIndex(table)).append(" ")
            .append("WHERE key");

        final Iterable<ODocument> items;
        if (parameters.size() == 1)
        {
            final OSQLSynchQuery cmd = new OSQLSynchQuery(query.append(" = ?").toString());
            items = this.database.command(cmd).execute(parameters.toArray());
        }
        else
        {
            query.append(" = [");
            for (int i = 0; i < parameters.size(); i++)
            {
                if (i > 0)
                {
                    query.append(",");
                }
                query.append("?");
            }
            query.append("]");
            items = this.query(query.toString(), parameters);
        }

        for (final ODocument item : items)
        {
            final OIdentifiable doc = fixIdentifiable(item);
            if (doc != null)
            {
                return doc;
            }
        }

        return null;
    }

    public final Collection<OIdentifiable> findIdByKeys(final EntityContext context, final Set<Object> keys)
    {
        if (null == context || null == keys || keys.isEmpty())
        {
            return Collections.emptyList();
        }

        final Collection<OIdentifiable> docs = new ArrayList<>(keys.size());
        for (final Object key : keys)
        {
            final OIdentifiable doc = this.findIdByKey(context, key);
            if (doc != null)
            {
                docs.add(doc);
            }
        }
        return Collections.unmodifiableCollection(docs);
    }

    public final OIdentifiable findIdByKey(final EntityContext context, final Object key)
    {
        if (null == context || null == key)
        {
            return null;
        }

        if (key instanceof OIdentifiable)
        {
            return fixIdentifiable((OIdentifiable) key);
        }

        for (final TableMeta table : context.getPrimaryTables())
        {
            final OIdentifiable doc = this.findIdByMap(table, context.getId().fromKey(key));
            if (doc != null)
            {
                return fixIdentifiable(doc);
            }
        }
        return null;
    }

    private OIdentifiable fixIdentifiable(final OIdentifiable record)
    {
        if (null == record)
        {
            return null;
        }

        if (record instanceof ODocument)
        {
            final ODocument doc = (ODocument) record;
            if (doc.fields() <= 1)
            {
                final Object value = doc.fieldValues()[0];
                if (value instanceof OIdentifiable)
                {
                    return findDocument((OIdentifiable) value);
                }
            }

            final Object field = doc.field("rid");
            if (field instanceof OIdentifiable)
            {
                return findDocument((OIdentifiable) field);
            }
        }

            return record;
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
