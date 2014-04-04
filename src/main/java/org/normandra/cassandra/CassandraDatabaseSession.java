package org.normandra.cassandra;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.DatabaseSession;
import org.normandra.NormandraException;
import org.normandra.cache.EntityCache;
import org.normandra.cache.MemoryCache;
import org.normandra.data.BasicDataHolder;
import org.normandra.data.ColumnAccessor;
import org.normandra.data.DataHolder;
import org.normandra.generator.IdGenerator;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.SingleEntityContext;
import org.normandra.meta.TableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a cassandra database session
 * <p>
 * User: bowen
 * Date: 2/1/14
 */
public class CassandraDatabaseSession implements DatabaseSession
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraDatabaseSession.class);

    private final String keyspaceName;

    private final Session session;

    private final EntityCache cache = new MemoryCache();

    private final List<RegularStatement> statements = new CopyOnWriteArrayList<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final AtomicBoolean activeUnitOfWork = new AtomicBoolean(false);


    public CassandraDatabaseSession(final String keyspace, final Session session)
    {
        if (null == keyspace || keyspace.isEmpty())
        {
            throw new IllegalArgumentException("Keyspace cannot be null/empty.");
        }
        if (null == session)
        {
            throw new NullArgumentException("session");
        }
        this.keyspaceName = keyspace;
        this.session = session;
    }


    @Override
    public void close()
    {
        // do nothing - session is thread safe and can be shared
        this.closed.getAndSet(true);
        this.cache.clear();
    }


    public boolean isClosed()
    {
        return this.closed.get();
    }


    public String getKeyspace()
    {
        return this.keyspaceName;
    }


    public Session getSession()
    {
        return this.session;
    }


    @Override
    public void clear() throws NormandraException
    {
        this.cache.clear();
    }


    @Override
    public void beginWork() throws NormandraException
    {
        if (this.activeUnitOfWork.get())
        {
            throw new IllegalStateException("Unit of work already active.");
        }
        this.activeUnitOfWork.getAndSet(true);
    }


    @Override
    public void commitWork() throws NormandraException
    {
        if (!this.activeUnitOfWork.get())
        {
            throw new IllegalStateException("No active transaction.");
        }

        final RegularStatement[] list = this.statements.toArray(new RegularStatement[this.statements.size()]);
        final Batch batch = QueryBuilder.batch(list);
        this.session.execute(batch);
        this.statements.clear();

        this.activeUnitOfWork.getAndSet(false);
    }


    @Override
    public void rollbackWork() throws NormandraException
    {
        if (!this.activeUnitOfWork.get())
        {
            throw new IllegalStateException("No active transaction.");
        }
        this.statements.clear();
        this.activeUnitOfWork.getAndSet(false);
    }


    @Override
    public boolean exists(final EntityContext meta, final Object key) throws NormandraException
    {
        if (this.isClosed())
        {
            throw new IllegalStateException("Session is closed.");
        }
        if (null == meta || null == key)
        {
            return false;
        }

        try
        {
            final Map<String, Object> columns = meta.getId().fromKey(key);
            final List<String> namelist = new ArrayList<>(columns.keySet());
            final TableMeta table = meta.getTables().iterator().next();
            final String[] names = namelist.toArray(new String[namelist.size()]);
            final Select statement = QueryBuilder.select(names)
                    .from(this.keyspaceName, table.getName())
                    .limit(1);
            boolean hasWhere = false;
            for (final Map.Entry<String, Object> entry : columns.entrySet())
            {
                final String name = entry.getKey();
                final Object value = entry.getValue();
                statement.where(QueryBuilder.eq(name, value));
                hasWhere = true;
            }
            if (!hasWhere)
            {
                logger.warn("Unable to #exists value without key - empty where statement for type [" + meta + "].");
                return false;
            }

            final ResultSet results = this.session.execute(statement);
            if (null == results)
            {
                return false;
            }
            final Row row = results.one();
            if (null == row)
            {
                return false;
            }

            final ColumnDefinitions definitions = row.getColumnDefinitions();
            return definitions != null && definitions.size() > 0;
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to query entity [" + meta + "] by key [" + key + "].", e);
        }
    }


    @Override
    public boolean exists(EntityMeta meta, Object key) throws NormandraException
    {
        return this.exists(new SingleEntityContext(meta), key);
    }


    @Override
    public Object get(final EntityContext meta, final Object key) throws NormandraException
    {
        if (this.isClosed())
        {
            throw new IllegalStateException("Session is closed.");
        }
        final CassandraEntityQuery query = new CassandraEntityQuery(this, this.cache);
        return query.query(meta, key);
    }


    @Override
    public Object get(EntityMeta meta, Object key) throws NormandraException
    {
        return this.get(new SingleEntityContext(meta), key);
    }


    @Override
    public List<Object> get(final EntityContext meta, final Object... keys) throws NormandraException
    {
        if (this.isClosed())
        {
            throw new IllegalStateException("Session is closed.");
        }
        final CassandraEntityQuery query = new CassandraEntityQuery(this, this.cache);
        return query.query(meta, keys);
    }


    @Override
    public List<Object> get(EntityMeta meta, Object... keys) throws NormandraException
    {
        return this.get(new SingleEntityContext(meta), keys);
    }


    @Override
    public void delete(final EntityMeta meta, final Object element) throws NormandraException
    {
        if (null == element)
        {
            throw new NullArgumentException("entity");
        }
        if (this.isClosed())
        {
            throw new IllegalStateException("Session is closed.");
        }

        try
        {
            final List<Delete> deletes = new ArrayList<>();
            for (final TableMeta table : meta)
            {
                boolean hasValue = false;
                final Delete statement = QueryBuilder.delete().all().from(this.keyspaceName, table.getName());
                for (final ColumnMeta column : table.getPrimaryKeys())
                {
                    final String name = column.getName();
                    final ColumnAccessor accessor = meta.getAccessor(column);
                    final Object value = accessor != null ? accessor.getValue(element) : null;
                    if (value != null)
                    {
                        hasValue = true;
                        statement.where(QueryBuilder.eq(name, value));
                    }
                }
                if (hasValue)
                {
                    deletes.add(statement);
                }
            }
            if (deletes.isEmpty())
            {
                throw new NormandraException("No column values found - cannot delete entity.");
            }
            if (this.activeUnitOfWork.get())
            {
                this.statements.addAll(deletes);
            }
            else
            {
                if (deletes.size() == 1)
                {
                    this.session.execute(deletes.get(0));
                }
                else
                {
                    final RegularStatement[] statements = deletes.toArray(new RegularStatement[deletes.size()]);
                    this.session.execute(QueryBuilder.batch(statements));
                }
            }
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to delete entity [" + element + "] of type [" + meta + "].", e);
        }
    }


    @Override
    public void save(final EntityMeta meta, final Object element) throws NormandraException
    {
        if (this.isClosed())
        {
            throw new IllegalStateException("Session is closed.");
        }
        if (null == meta)
        {
            throw new NullArgumentException("entity metadata");
        }
        if (null == element)
        {
            throw new NullArgumentException("element");
        }

        try
        {
            // generate any primary ids
            final EntityContext ctx = new SingleEntityContext(meta);
            for (final TableMeta table : meta)
            {
                for (final ColumnMeta column : table.getColumns())
                {
                    final ColumnAccessor accessor = meta.getAccessor(column);
                    final IdGenerator generator = meta.getGenerator(column);
                    if (generator != null && accessor != null && accessor.isEmpty(element))
                    {
                        final Object generated = generator.generate(meta);
                        final DataHolder data = new BasicDataHolder(generated);
                        accessor.setValue(element, data, this);
                    }
                }
            }
            // generate insert/updateInstance statements
            final List<Insert> inserts = new ArrayList<>();
            final List<Delete> deletes = new ArrayList<>();
            for (final TableMeta table : meta)
            {
                if (table.getPrimaryKeys().size() == ctx.getPrimaryKeys().size())
                {
                    // this table has the same number of keys
                    Insert statement = QueryBuilder.insertInto(this.keyspaceName, table.getName());
                    boolean hasValue = false;
                    for (final ColumnMeta column : table.getColumns())
                    {
                        final ColumnAccessor accessor = meta.getAccessor(column);
                        if (accessor != null && accessor.isLoaded(element))
                        {
                            final boolean empty = accessor.isEmpty(element);
                            final Object value = !empty ? accessor.getValue(element) : null;
                            if (value != null)
                            {
                                statement = statement.value(column.getName(), value);
                                hasValue = true;
                            }
                            else
                            {
                                final Delete delete = QueryBuilder.delete(column.getName()).from(this.keyspaceName, table.getName());
                                for (final ColumnMeta key : table.getPrimaryKeys())
                                {
                                    final ColumnAccessor keyAccessor = meta.getAccessor(key);
                                    final Object keyValue = keyAccessor != null ? keyAccessor.getValue(element) : null;
                                    if (keyValue != null)
                                    {
                                        delete.where(QueryBuilder.eq(key.getName(), keyValue));
                                    }

                                }
                                deletes.add(delete);
                            }
                        }
                    }
                    if (hasValue)
                    {
                        inserts.add(statement);
                    }
                }
                else
                {
                    // this table as an extra set of keys, likely as a join table
                    final Map<ColumnMeta, Object> keys = new TreeMap<>();
                    for (final ColumnMeta column : ctx.getPrimaryKeys())
                    {
                        if (table.hasColumn(column.getName()))
                        {
                            final ColumnAccessor accessor = meta.getAccessor(column);
                            final Object value = accessor != null ? accessor.getValue(element) : null;
                            if (value != null)
                            {
                                keys.put(column, value);
                            }
                        }
                    }
                    final Set<ColumnMeta> extraKeys = new TreeSet<>(table.getPrimaryKeys());
                    extraKeys.removeAll(ctx.getPrimaryKeys());
                    for (final ColumnMeta column : extraKeys)
                    {
                        final ColumnAccessor accessor = meta.getAccessor(column);
                        if (accessor.isLoaded(element))
                        {
                            final boolean empty = accessor.isEmpty(element);
                            final Object value = !empty && accessor != null ? accessor.getValue(element) : null;
                            if (value instanceof Collection)
                            {
                                for (final Object item : ((Collection) value))
                                {
                                    Insert statement = QueryBuilder.insertInto(this.keyspaceName, table.getName());
                                    for (final Map.Entry<ColumnMeta, Object> entry : keys.entrySet())
                                    {
                                        statement = statement.value(entry.getKey().getName(), entry.getValue());
                                    }
                                    statement = statement.value(column.getName(), item);
                                    inserts.add(statement);
                                }
                            }
                            else if (value != null)
                            {
                                Insert statement = QueryBuilder.insertInto(this.keyspaceName, table.getName());
                                for (final Map.Entry<ColumnMeta, Object> entry : keys.entrySet())
                                {
                                    statement = statement.value(entry.getKey().getName(), entry.getValue());
                                }
                                statement = statement.value(column.getName(), value);
                                inserts.add(statement);
                            }
                            else
                            {
                                final Delete delete = QueryBuilder.delete(column.getName()).from(this.keyspaceName, table.getName());
                                for (final Map.Entry<ColumnMeta, Object> entry : keys.entrySet())
                                {
                                    delete.where(QueryBuilder.eq(entry.getKey().getName(), entry.getValue()));
                                }
                                deletes.add(delete);
                            }
                        }
                    }
                }
            }
            if (inserts.isEmpty())
            {
                throw new NormandraException("No column values found - cannot save empty entity.");
            }
            if (this.activeUnitOfWork.get())
            {
                this.statements.addAll(inserts);
                this.statements.addAll(deletes);
            }
            else
            {
                if (inserts.size() == 1)
                {
                    this.session.execute(inserts.get(0));
                }
                else
                {
                    final List<RegularStatement> batch = new ArrayList<>(inserts.size() + deletes.size());
                    batch.addAll(inserts);
                    batch.addAll(deletes);
                    final RegularStatement[] statements = batch.toArray(new RegularStatement[batch.size()]);
                    this.session.execute(QueryBuilder.batch(statements));
                }
            }
            this.cache.put(meta, element);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to save entity [" + meta + "] instance [" + element + "].", e);
        }
    }
}
