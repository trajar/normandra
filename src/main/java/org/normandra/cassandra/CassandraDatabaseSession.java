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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.functors.InstanceofPredicate;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.NormandraDatabaseSession;
import org.normandra.NormandraException;
import org.normandra.cache.EntityCache;
import org.normandra.cache.MemoryCache;
import org.normandra.data.ColumnAccessor;
import org.normandra.generator.IdGenerator;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.DiscriminatorMeta;
import org.normandra.meta.EntityMeta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a cassandra database session
 * <p/>
 * User: bowen
 * Date: 2/1/14
 */
public class CassandraDatabaseSession implements NormandraDatabaseSession
{
    private final String keyspaceName;

    private final Session session;

    private final EntityCache cache = new MemoryCache();

    private final List<RegularStatement> statements = new CopyOnWriteArrayList<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final AtomicBoolean activeTransaction = new AtomicBoolean(false);


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
        this.statements.clear();
    }


    @Override
    public void beginTransaction() throws NormandraException
    {
        if (this.activeTransaction.get())
        {
            throw new IllegalStateException("Transaction already active.");
        }
        this.activeTransaction.getAndSet(true);
    }


    @Override
    public void commitTransaction() throws NormandraException
    {
        if (!this.activeTransaction.get())
        {
            throw new IllegalStateException("No active transaction.");
        }

        final RegularStatement[] list = this.statements.toArray(new RegularStatement[this.statements.size()]);
        final Batch batch = QueryBuilder.batch(list);
        this.session.execute(batch);
        this.statements.clear();

        this.activeTransaction.getAndSet(false);
    }


    @Override
    public void rollbackTransaction() throws NormandraException
    {
        if (!this.activeTransaction.get())
        {
            throw new IllegalStateException("No active transaction.");
        }
        this.statements.clear();
        this.activeTransaction.getAndSet(false);
    }


    @Override
    public <T> boolean exists(final EntityMeta<T> meta, final Object key) throws NormandraException
    {
        if (this.isClosed())
        {
            throw new IllegalStateException("Session is closed.");
        }
        if (null == meta || null == key)
        {
            return false;
        }

        final ColumnMeta partition = meta.getPartition();
        if (null == partition)
        {
            return false;
        }

        try
        {
            final String[] names = new String[]{partition.getName()};
            final Select statement = QueryBuilder.select(names)
                    .from(this.keyspaceName, meta.getTable())
                    .where(QueryBuilder.eq(partition.getName(), key)).limit(1);
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
            if (null == definitions)
            {
                return false;
            }
            return definitions.size() > 0;
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to query entity [" + meta + "] by key [" + key + "].", e);
        }
    }


    @Override
    public <T> Object discriminator(final EntityMeta<T> meta, final Object key) throws NormandraException
    {
        if (this.isClosed())
        {
            throw new IllegalStateException("Session is closed.");
        }
        if (null == meta || null == key)
        {
            return null;
        }

        final ColumnMeta partition = meta.getPartition();
        if (null == partition)
        {
            return null;
        }

        final DiscriminatorMeta descrim = (DiscriminatorMeta) CollectionUtils.find(meta.getColumns(), InstanceofPredicate.getInstance(DiscriminatorMeta.class));
        if (null == descrim)
        {
            return null;
        }

        try
        {
            final String[] names = new String[]{descrim.getName()};
            final Select statement = QueryBuilder.select(names)
                    .from(this.keyspaceName, meta.getTable())
                    .where(QueryBuilder.eq(partition.getName(), key)).limit(1);
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
            return CassandraUtils.unpack(row, 0, descrim);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get entity [" + meta + "] by key [" + key + "].", e);
        }
    }


    @Override
    public <T> T get(final EntityMeta<T> meta, final Object key) throws NormandraException
    {
        if (this.isClosed())
        {
            throw new IllegalStateException("Session is closed.");
        }
        if (null == meta || null == key)
        {
            return null;
        }

        final ColumnMeta partition = meta.getPartition();
        if (null == partition)
        {
            return null;
        }

        try
        {
            final List<ColumnMeta> columns = new ArrayList<>(meta.getColumns());
            final String[] names = new String[columns.size()];
            for (int i = 0; i < columns.size(); i++)
            {
                names[i] = columns.get(i).getName();
            }
            final Select statement = QueryBuilder.select(names)
                    .from(this.keyspaceName, meta.getTable())
                    .where(QueryBuilder.eq(partition.getName(), key)).limit(1);
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

            final T entity = meta.getType().newInstance();
            if (null == entity)
            {
                return null;
            }
            for (int i = 0; i < names.length; i++)
            {
                final ColumnMeta column = columns.get(i);
                final Object value = CassandraUtils.unpack(row, i, column);
                if (value != null)
                {
                    column.getAccessor().setValue(entity, value, this);
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
    public <T> void delete(final EntityMeta<T> meta, final T element) throws NormandraException
    {
        if (this.isClosed())
        {
            throw new IllegalStateException("Session is closed.");
        }

        final ColumnMeta partition = meta.getPartition();
        if (null == partition)
        {
            throw new NormandraException("Unable to determine the partiation key for [" + meta + "].");
        }

        final Object key = partition.getAccessor().getValue(element);
        if (null == key)
        {
            throw new NormandraException("Entity [" + meta + "] instance [" + element + "] has null/empty partition key.");
        }

        try
        {
            final Delete statement = QueryBuilder.delete().all().from(this.keyspaceName, meta.getTable());
            statement.where(QueryBuilder.eq(partition.getName(), key));
            if (this.activeTransaction.get())
            {
                this.statements.add(statement);
            }
            else
            {
                this.session.execute(statement);
            }
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to delete entity [" + meta + "] by key [" + key + "].", e);
        }
    }


    @Override
    public <T> void save(final EntityMeta<T> meta, final T element) throws NormandraException
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
            boolean hasValue = false;
            Insert statement = QueryBuilder.insertInto(this.keyspaceName, meta.getTable());
            final Collection<ColumnMeta> columns = meta.getColumns();
            for (final ColumnMeta column : columns)
            {
                final ColumnAccessor accessor = column.getAccessor();
                final IdGenerator generator = meta.getGenerator(column);
                final Object value;
                if (generator != null && accessor.isEmpty(element))
                {
                    final Object generated = generator.generate(meta);
                    if (generated != null)
                    {
                        accessor.setValue(element, generated, this);
                    }
                    value = generated;
                }
                else
                {
                    value = accessor.getValue(element);
                }

                if (value != null)
                {
                    statement = statement.value(column.getName(), value);
                    hasValue = true;
                }
            }
            if (!hasValue)
            {
                throw new NormandraException("No column values found - cannot save empty entity.");
            }
            if (this.activeTransaction.get())
            {
                this.statements.add(statement);
            }
            else
            {
                this.session.execute(statement);
            }
            this.cache.put(meta, element);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to save entity [" + meta + "] instance [" + element + "].", e);
        }
    }
}
