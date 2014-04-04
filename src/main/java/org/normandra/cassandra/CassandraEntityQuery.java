package org.normandra.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.normandra.NormandraException;
import org.normandra.cache.EntityCache;
import org.normandra.data.ColumnAccessor;
import org.normandra.data.DataHolder;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;
import org.normandra.util.ArraySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a class capable of querying and constructing entity instances
 * <p>
 * User: bowen
 * Date: 3/23/14
 */
public class CassandraEntityQuery
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraEntityQuery.class);

    private final CassandraDatabaseSession session;

    private final EntityCache cache;


    public CassandraEntityQuery(final CassandraDatabaseSession session, final EntityCache cache)
    {
        this.session = session;
        this.cache = cache;
    }


    public Object query(final EntityContext meta, final Object key) throws NormandraException
    {
        if (null == meta || null == key)
        {
            return null;
        }

        if (key instanceof Serializable)
        {
            // check cache
            for (final EntityMeta entity : meta.getEntities())
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
            // pull primary tables necessary
            final Map<TableMeta, Row> rows = new TreeMap<>();
            final Map<ColumnMeta, Object> data = new TreeMap<>();
            for (final TableMeta table : meta.getPrimaryTables())
            {
                final ResultSet results = this.buildEagerQuery(meta, table, key);
                final Row row = results != null ? results.one() : null;
                if (row != null)
                {
                    rows.put(table, row);
                    data.putAll(CassandraUtils.unpackValues(table, row));
                }
            }
            if (rows.isEmpty() || data.isEmpty())
            {
                return null;
            }

            // create new instance, copy values
            final EntityMeta entity = meta.findEntity(data);
            if (null == entity)
            {
                return null;
            }
            final Object instance = entity.getType().newInstance();
            if (null == instance)
            {
                return null;
            }
            if (!CassandraUtils.updateInstance(entity, instance, rows, this.session))
            {
                return null;
            }
            this.cache.put(entity, instance);

            // now pull secondary tables
            for (final TableMeta table : meta.getSecondaryTables())
            {
                final ResultSet results = this.buildEagerQuery(meta, table, key);
                if (results != null)
                {
                    CassandraUtils.updateInstance(entity, instance, table, results, this.session);
                }
            }

            // setup lazy loaded properties
            for (final TableMeta table : meta.getTables())
            {
                for (final ColumnMeta column : table)
                {
                    if (column.isLazyLoaded())
                    {
                        final ColumnAccessor accessor = entity.getAccessor(column);
                        if (accessor != null)
                        {
                            accessor.setValue(instance, new LazyDataHolder(this.session, entity, table, column, key), this.session);
                        }
                    }
                }
            }

            // done
            return instance;
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get entity [" + meta + "] by key [" + key + "].", e);
        }
    }


    public List<Object> query(final EntityContext meta, final Object... keys) throws NormandraException
    {
        if (null == meta || null == keys || keys.length <= 0)
        {
            return null;
        }

        final List<Object> entities = new ArrayList<>(keys.length);
        for (final Object key : keys)
        {
            for (final EntityMeta entity : meta.getEntities())
            {
                if (key instanceof Serializable)
                {
                    // check cache
                    final Object existing = this.cache.get(entity, (Serializable) key);
                    if (existing != null)
                    {
                        entities.add(existing);
                    }
                }
            }
        }
        if (entities.size() == keys.length)
        {
            return Collections.unmodifiableList(entities);
        }

        try
        {
            // query each table as necessary
            final Map<Object, KeyContext> keymap = new HashMap<>(keys.length);
            for (final TableMeta table : meta.getPrimaryTables())
            {
                final ResultSet results = this.buildEagerQuery(meta, table, keys);
                if (results != null)
                {
                    for (final Row row : results)
                    {
                        final Map<ColumnMeta, Object> data = CassandraUtils.unpackValues(table, row);
                        final EntityMeta entity = meta.findEntity(data);
                        if (entity != null)
                        {
                            final Map<String, Object> dataByName = new HashMap<>(data.size());
                            for (final Map.Entry<ColumnMeta, Object> entry : data.entrySet())
                            {
                                dataByName.put(entry.getKey().getName(), entry.getValue());
                            }
                            final Object key = entity.getId().toKey(dataByName);
                            if (key != null)
                            {
                                KeyContext ctx = keymap.get(key);
                                if (null == ctx)
                                {
                                    ctx = new KeyContext(entity, table);
                                    keymap.put(key, ctx);
                                }
                                ctx.rows.add(row);
                            }
                        }
                    }
                }
            }
            if (keymap.isEmpty())
            {
                return null;
            }

            // create new instance, copy values
            for (final KeyContext ctx : keymap.values())
            {
                final List<Row> rows = ctx.rows;
                final TableMeta table = ctx.table;
                final Object entity = ctx.entity.getType().newInstance();
                for (final Row row : rows)
                {
                    CassandraUtils.updateInstance(ctx.entity, entity, table, row, this.session);
                }
                this.cache.put(ctx.entity, entity);
                entities.add(entity);
            }
            return Collections.unmodifiableList(entities);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get entity [" + meta + "] by keys [" + keys + "].", e);
        }
    }


    private ResultSet buildEagerQuery(final EntityContext meta, final TableMeta table, final Object... keys) throws NormandraException
    {
        // get columns to query
        final Set<ColumnMeta> columns = new ArraySet<>(meta.getColumns());
        columns.retainAll(table.getEagerLoaded());
        if (table.isSecondary())
        {
            // for secondary tables, only query extra fields
            columns.removeAll(meta.getPrimaryKeys());
        }
        if (columns.isEmpty())
        {
            return null;
        }
        final String[] names = new String[columns.size()];
        int i = 0;
        for (final ColumnMeta column : columns)
        {
            names[i] = column.getName();
            i++;
        }

        // setup select statement
        final Select statement = QueryBuilder.select(names).from(this.session.getKeyspace(), table.getName());
        boolean hasWhere = false;
        if (keys.length == 1)
        {
            for (final Map.Entry<String, Object> entry : meta.getId().fromKey(keys[0]).entrySet())
            {
                final String name = entry.getKey();
                final Object value = entry.getValue();
                statement.where(QueryBuilder.eq(name, value));
                hasWhere = true;
            }
        }
        else if (keys.length > 0)
        {
            final Map<String, List<Object>> values = new TreeMap<>();
            for (final Object key : keys)
            {
                for (final Map.Entry<String, Object> entry : meta.getId().fromKey(key).entrySet())
                {
                    final String name = entry.getKey();
                    final Object value = entry.getValue();
                    if (value != null)
                    {
                        List<Object> list = values.get(name);
                        if (null == list)
                        {
                            list = new ArrayList<>();
                            values.put(name, list);
                        }
                        list.add(value);
                    }
                }
            }
            for (final Map.Entry<String, List<Object>> entry : values.entrySet())
            {
                final String name = entry.getKey();
                final List list = entry.getValue();
                if (!list.isEmpty())
                {
                    statement.where(QueryBuilder.in(name, list.toArray()));
                    hasWhere = true;
                }
            }
        }
        if (!hasWhere)
        {
            logger.warn("Unable to #get value without key - empty where statement for type [" + meta + "].");
            return null;
        }

        // add results
        return this.session.getSession().execute(statement);
    }


    private static class KeyContext
    {
        private final EntityMeta entity;

        private final TableMeta table;

        private final List<Row> rows = new ArrayList<>();


        private KeyContext(final EntityMeta meta, final TableMeta table)
        {
            this.entity = meta;
            this.table = table;
        }
    }

    private static class LazyDataHolder implements DataHolder
    {
        private final AtomicBoolean loaded = new AtomicBoolean(false);

        private final CassandraDatabaseSession session;

        private final EntityMeta entity;

        private final TableMeta table;

        private final ColumnMeta column;

        private final Object key;

        private final List<Row> rows = new ArrayList<>();


        private LazyDataHolder(final CassandraDatabaseSession session, final EntityMeta meta, final TableMeta table, final ColumnMeta column, final Object key)
        {
            this.session = session;
            this.entity = meta;
            this.table = table;
            this.column = column;
            this.key = key;
        }


        @Override
        public boolean isEmpty()
        {
            try
            {
                return this.ensureResults().isEmpty();
            }
            catch (final Exception e)
            {
                throw new IllegalStateException("Unable to query lazy loaded results from table [" + this.table + "] column [" + this.column + "].", e);
            }
        }


        @Override
        public Object get() throws NormandraException
        {
            final List<Row> rows = this.ensureResults();
            if (null == rows || rows.isEmpty())
            {
                return null;
            }
            try
            {
                return CassandraUtils.unpackValue(rows, this.column.getName(), this.column);
            }
            catch (final Exception e)
            {
                throw new NormandraException("Unable to unpack lazy loaded results for column [" + this.column + "] on entity [" + this.entity + "].", e);
            }
        }


        private List<Row> ensureResults() throws NormandraException
        {
            if (this.loaded.get())
            {
                return this.rows;
            }
            synchronized (this)
            {
                if (this.loaded.get())
                {
                    return this.rows;
                }
                final Select statement = QueryBuilder.select(this.column.getName()).from(this.session.getKeyspace(), this.table.getName());
                boolean hasWhere = false;
                for (final Map.Entry<String, Object> entry : this.entity.getId().fromKey(this.key).entrySet())
                {
                    final String name = entry.getKey();
                    final Object value = entry.getValue();
                    statement.where(QueryBuilder.eq(name, value));
                    hasWhere = true;
                }
                if (hasWhere)
                {
                    this.rows.addAll(this.session.getSession().execute(statement).all());
                }
                this.loaded.getAndSet(true);
            }
            return this.rows;
        }
    }
}
