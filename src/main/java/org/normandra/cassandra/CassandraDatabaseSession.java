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
import org.normandra.data.ColumnAccessor;
import org.normandra.generator.IdGenerator;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.DiscriminatorMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a cassandra database session
 * <p/>
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
    public boolean exists(final EntityMeta meta, final Object key) throws NormandraException
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
            final List<String> namelist = new ArrayList<>(columns.size());
            for (final String column : columns.keySet())
            {
                namelist.add(column);
            }
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
    public Object discriminator(final EntityMeta meta, final Object key) throws NormandraException
    {
        if (this.isClosed())
        {
            throw new IllegalStateException("Session is closed.");
        }
        if (null == meta || null == key)
        {
            return null;
        }

        final DiscriminatorMeta descrim = meta.getDiscriminator();
        if (null == descrim)
        {
            return null;
        }

        try
        {
            // find table that holds discriminator
            TableMeta descrimTable = null;
            for (final TableMeta table : meta)
            {
                if (table.hasColumn(descrim.getName()))
                {
                    descrimTable = table;
                    break;
                }
            }
            if (null == descrimTable)
            {
                return null;
            }

            // query table
            final String[] names = new String[]{descrim.getName()};
            final Select statement = QueryBuilder.select(names)
                    .from(this.keyspaceName, descrimTable.getName())
                    .limit(1);
            for (final Map.Entry<String, Object> entry : meta.getId().fromKey(key).entrySet())
            {
                final String name = entry.getKey();
                final Object value = entry.getValue();
                statement.where(QueryBuilder.eq(name, value));
            }
            final ResultSet results = this.session.execute(statement);
            if (null == results)
            {
                return null;
            }
            final Row row = results.one();
            if (null == row)
            {
                return null;
            }
            return CassandraUtils.unpack(row, descrim.getName(), descrim);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get entity [" + meta + "] by key [" + key + "].", e);
        }
    }


    @Override
    public Object get(final EntityMeta meta, final Object key) throws NormandraException
    {
        if (this.isClosed())
        {
            throw new IllegalStateException("Session is closed.");
        }
        if (null == meta || null == key)
        {
            return null;
        }

        if (key instanceof Serializable)
        {
            // check cache
            final Object existing = this.cache.get(meta, (Serializable) key);
            if (existing != null)
            {
                return meta.getType().cast(existing);
            }
        }

        try
        {
            // query each table as necessary
            final Map<TableMeta, Row> rows = new TreeMap<>();
            for (final TableMeta table : meta)
            {
                // get columns to query
                final List<ColumnMeta> columns = new ArrayList<>(table.getColumns());
                final String[] names = new String[columns.size()];
                for (int i = 0; i < columns.size(); i++)
                {
                    names[i] = columns.get(i).getName();
                }

                // setup select statement
                final Select statement = QueryBuilder.select(names)
                        .from(this.keyspaceName, table.getName())
                        .limit(1);
                boolean hasWhere = false;
                for (final Map.Entry<String, Object> entry : meta.getId().fromKey(key).entrySet())
                {
                    final String name = entry.getKey();
                    final Object value = entry.getValue();
                    statement.where(QueryBuilder.eq(name, value));
                    hasWhere = true;
                }
                if (!hasWhere)
                {
                    logger.warn("Unable to #get value without key - empty where statement for type [" + meta + "].");
                    return null;
                }

                // add results
                final ResultSet results = this.session.execute(statement);
                final Row row = results != null ? results.one() : null;
                if (row != null)
                {
                    rows.put(table, row);
                }
            }
            if (rows.isEmpty())
            {
                return null;
            }

            // create new instance, copy values
            final Object entity = meta.getType().newInstance();
            if (null == entity)
            {
                return null;
            }
            for (final Map.Entry<TableMeta, Row> entry : rows.entrySet())
            {
                final TableMeta table = entry.getKey();
                final Row row = entry.getValue();
                final ColumnDefinitions defs = row.getColumnDefinitions();
                for (final ColumnDefinitions.Definition def : defs.asList())
                {
                    final String columnName = def.getName();
                    final ColumnMeta column = table.getColumn(columnName);
                    final ColumnAccessor accessor = meta.getAccessor(columnName);
                    if (accessor != null)
                    {
                        final Object value = CassandraUtils.unpack(row, columnName, column);
                        if (value != null)
                        {
                            accessor.setValue(entity, value, this);
                        }
                    }
                }
            }
            this.cache.put(meta, entity);
            return entity;
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get entity [" + meta + "] by key [" + key + "].", e);
        }
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
            final List<Insert> inserts = new ArrayList<>();
            for (final TableMeta table : meta)
            {
                boolean hasValue = false;
                Insert statement = QueryBuilder.insertInto(this.keyspaceName, table.getName());
                final Collection<ColumnMeta> columns = table.getColumns();
                for (final ColumnMeta column : columns)
                {
                    final ColumnAccessor accessor = meta.getAccessor(column);
                    final IdGenerator generator = meta.getGenerator(column);
                    Object value = null;
                    if (generator != null && accessor != null && accessor.isEmpty(element))
                    {
                        final Object generated = generator.generate(meta);
                        if (generated != null)
                        {
                            accessor.setValue(element, generated, this);
                        }
                        value = generated;
                    }
                    else if (accessor != null)
                    {
                        value = accessor.getValue(element);
                    }
                    if (value != null)
                    {
                        statement = statement.value(column.getName(), value);
                        hasValue = true;
                    }
                }
                if (hasValue)
                {
                    inserts.add(statement);
                }
            }
            if (inserts.isEmpty())
            {
                throw new NormandraException("No column values found - cannot save empty entity.");
            }
            if (this.activeUnitOfWork.get())
            {
                this.statements.addAll(inserts);
            }
            else
            {
                if (inserts.size() == 1)
                {
                    this.session.execute(inserts.get(0));
                }
                else
                {
                    final RegularStatement[] statements = inserts.toArray(new RegularStatement[inserts.size()]);
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
