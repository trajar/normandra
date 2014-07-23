package org.normandra.cassandra;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.DatabaseQuery;
import org.normandra.DatabaseSession;
import org.normandra.NormandraException;
import org.normandra.cache.EntityCache;
import org.normandra.data.BasicDataHolder;
import org.normandra.data.ColumnAccessor;
import org.normandra.data.DataHolder;
import org.normandra.generator.IdGenerator;
import org.normandra.log.DatabaseActivity;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.SingleEntityContext;
import org.normandra.meta.TableMeta;
import org.normandra.util.EntityPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
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

    private final EntityCache cache;

    private final List<RegularStatement> statements = new CopyOnWriteArrayList<>();

    private final List<DatabaseActivity> activities = new CopyOnWriteArrayList<>();

    private final Map<String, CassandraPreparedStatement> preparedStatements;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final AtomicBoolean activeUnitOfWork = new AtomicBoolean(false);

    private final Executor executor;


    public CassandraDatabaseSession(final String keyspace, final Session session, final Map<String, CassandraPreparedStatement> map, final EntityCache cache, final Executor executor)
    {
        if (null == keyspace || keyspace.isEmpty())
        {
            throw new IllegalArgumentException("Keyspace cannot be null/empty.");
        }
        if (null == session)
        {
            throw new NullArgumentException("session");
        }
        if (null == cache)
        {
            throw new NullArgumentException("cache");
        }
        if (null == executor)
        {
            throw new NullArgumentException("executor");
        }
        this.keyspaceName = keyspace;
        this.session = session;
        this.cache = cache;
        this.executor = executor;
        this.preparedStatements = new TreeMap<>(map);
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


    protected ResultSet executeSync(final Statement statement, final DatabaseActivity.Type type)
    {
        final CassandraDatabaseActivity activity = new CassandraDatabaseActivity(statement, this.session, type);
        this.activities.add(activity);
        return activity.execute();
    }


    protected Future<ResultSet> executeAsync(final Statement statement, final DatabaseActivity.Type type)
    {
        final CassandraDatabaseActivity activity = new CassandraDatabaseActivity(statement, this.session, type);
        this.activities.add(activity);
        final Callable<ResultSet> callable = new Callable<ResultSet>()
        {
            @Override
            public ResultSet call() throws Exception
            {
                return activity.execute();
            }
        };
        final FutureTask<ResultSet> task = new FutureTask<>(callable);
        this.executor.execute(task);
        return task;
    }


    @Override
    public void clear()
    {
        this.activities.clear();
        this.cache.clear();
    }


    @Override
    public boolean pendingWork()
    {
        return this.activeUnitOfWork.get();
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
        if (this.statements.isEmpty())
        {
            return;
        }
        try
        {
            for (final RegularStatement statement : this.statements)
            {
                DatabaseActivity.Type type = DatabaseActivity.Type.ADMIN;
                if (statement instanceof Batch)
                {
                    type = DatabaseActivity.Type.BATCH;
                }
                else if (statement instanceof Delete)
                {
                    type = DatabaseActivity.Type.DELETE;
                }
                else if (statement instanceof Select)
                {
                    type = DatabaseActivity.Type.SELECT;
                }
                else if (statement instanceof Insert || statement instanceof Update)
                {
                    type = DatabaseActivity.Type.UPDATE;
                }
                this.executeSync(statement, type);
            }
            this.statements.clear();
            this.activeUnitOfWork.getAndSet(false);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to commit batch unit of work.", e);
        }
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
    public List<DatabaseActivity> listActivity()
    {
        return Collections.unmodifiableList(this.activities);
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

            final ResultSet results = this.executeSync(statement, DatabaseActivity.Type.SELECT);
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

            final RegularStatement batch;
            if (deletes.size() == 1)
            {
                batch = deletes.get(0);
            }
            else
            {
                final RegularStatement[] statements = deletes.toArray(new RegularStatement[deletes.size()]);
                batch = QueryBuilder.batch(statements);
            }

            final Object key = meta.getId().fromEntity(element);
            if (key instanceof Serializable)
            {
                this.cache.remove(meta, (Serializable) key);
            }

            if (this.activeUnitOfWork.get())
            {
                this.statements.add(batch);
            }
            else
            {
                this.executeSync(batch, DatabaseActivity.Type.DELETE);
            }
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to delete entity [" + element + "] of type [" + meta + "].", e);
        }
    }


    @Override
    public DatabaseQuery executeNamedQuery(final EntityContext meta, final String name, final Map<String, Object> parameters) throws NormandraException
    {
        final CassandraPreparedStatement prepared = this.preparedStatements.get(name);
        if (null == prepared)
        {
            throw new NormandraException("Unable to locate query [" + name + "].");
        }
        return new CassandraDatabaseQuery<>(meta, prepared.bind(parameters), this);
    }


    @Override
    public DatabaseQuery executeDynamciQuery(final EntityContext meta, final String query, final Map<String, Object> parameters) throws NormandraException
    {
        return new CassandraQueryParser(meta, this).parse(query, parameters);
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
            final CassandraDataHandler helper = new CassandraDataHandler(this);
            new EntityPersistence(this).save(meta, element, helper);
            final List<RegularStatement> operations = helper.getOperations();
            if (operations.isEmpty())
            {
                throw new IllegalStateException("No operations or columns identify for update - cannot save empty entity.");
            }
            final RegularStatement batch;
            if (operations.size() == 1)
            {
                batch = operations.get(0);
            }
            else
            {
                final RegularStatement[] statements = operations.toArray(new RegularStatement[operations.size()]);
                batch = QueryBuilder.batch(statements);
            }
            if (this.activeUnitOfWork.get())
            {
                this.statements.add(batch);
            }
            else
            {
                this.executeSync(batch, DatabaseActivity.Type.UPDATE);
            }

            final Object key = meta.getId().fromEntity(element);
            if (key instanceof Serializable)
            {
                this.cache.put(meta, (Serializable) key, element);
            }
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to save entity [" + meta + "] instance [" + element + "].", e);
        }
    }
}
