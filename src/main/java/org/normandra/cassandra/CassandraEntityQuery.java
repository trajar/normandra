package org.normandra.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Future;
import org.normandra.NormandraException;
import org.normandra.cache.EntityCache;
import org.normandra.log.DatabaseActivity;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;
import org.normandra.util.ArraySet;
import org.normandra.util.EntityBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        // check cache
        final Object existing = this.cache.get(meta, key, Object.class);
        if (existing != null)
        {
            return existing;
        }

        try
        {
            // pull primary tables necessary
            final Map<TableMeta, Future<ResultSet>> primaryFutures = new TreeMap<>();
            for (final TableMeta table : meta.getPrimaryTables())
            {
                final Future<ResultSet> future = this.buildEagerQuery(meta, table, key);
                if (future != null)
                {
                    primaryFutures.put(table, future);
                }
            }
            final Map<ColumnMeta, Object> data = new TreeMap<>();
            for (final Map.Entry<TableMeta, Future<ResultSet>> entry : primaryFutures.entrySet())
            {
                final TableMeta table = entry.getKey();
                final ResultSet results = entry.getValue().get();
                final Row row = results != null ? results.one() : null;
                if (row != null)
                {
                    data.putAll(CassandraUtils.unpackValues(table, row));
                }
            }
            if (data.isEmpty())
            {
                return null;
            }

            // create new instance, copy values
            final EntityMeta entity = meta.findEntity(data);
            if (null == entity)
            {
                return null;
            }
            final Object instance = new EntityBuilder(this.session, new CassandraDataFactory(this.session)).build(meta, data);
            if (null == instance)
            {
                return null;
            }
            this.cache.put(entity, key, instance);

            // done
            return instance;
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get entity [" + meta + "] by key [" + key + "].", e);
        }
    }


    public List<Object> query(final EntityContext context, final Object... keys) throws NormandraException
    {
        if (null == context || null == keys || keys.length <= 0)
        {
            return null;
        }

        final List<Object> entities = new ArrayList<>(keys.length);
        for (final Object key : keys)
        {
            for (final EntityMeta entity : context.getEntities())
            {
                if (key instanceof Serializable)
                {
                    // check cache
                    final Object existing = this.cache.get(entity, key, Object.class);
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
            final Map<TableMeta, Future<ResultSet>> primaryFutures = new TreeMap<>();
            for (final TableMeta table : context.getPrimaryTables())
            {
                final Future<ResultSet> future = this.buildEagerQuery(context, table, keys);
                if (future != null)
                {
                    primaryFutures.put(table, future);
                }
            }
            final Map<Object, KeyContext> keymap = new HashMap<>(keys.length);
            for (final Map.Entry<TableMeta, Future<ResultSet>> entry : primaryFutures.entrySet())
            {
                final TableMeta table = entry.getKey();
                final ResultSet results = entry.getValue().get();
                if (results != null)
                {
                    for (final Row row : results)
                    {
                        final Map<ColumnMeta, Object> data = CassandraUtils.unpackValues(table, row);
                        final EntityMeta entity = context.findEntity(data);
                        if (entity != null)
                        {
                            final Map<String, Object> dataByName = new HashMap<>(data.size());
                            for (final Map.Entry<ColumnMeta, Object> pair : data.entrySet())
                            {
                                dataByName.put(pair.getKey().getName(), pair.getValue());
                            }
                            final Object key = entity.getId().toKey(dataByName);
                            if (key != null)
                            {
                                KeyContext ctx = keymap.get(key);
                                if (null == ctx)
                                {
                                    ctx = new KeyContext(entity);
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
                final Map<ColumnMeta, Object> data = new TreeMap<>();
                for (final Row row : ctx.rows)
                {
                    final TableMeta table = ctx.entity.getTable(row.getColumnDefinitions().getTable(0));
                    data.putAll(CassandraUtils.unpackValues(table, row));
                }
                final Object instance = new EntityBuilder(this.session, new CassandraDataFactory(this.session)).build(context, data);
                if (instance != null)
                {
                    final Object key = context.getId().fromEntity(instance);
                    this.cache.put(ctx.entity, key, instance);
                    entities.add(instance);
                }
            }
            return Collections.unmodifiableList(entities);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get entity [" + context + "] by keys " + Arrays.asList(keys) + ".", e);
        }
    }


    private Future<ResultSet> buildEagerQuery(final EntityContext meta, final TableMeta table, final Object... keys) throws NormandraException
    {
        // get columns to query
        final Set<ColumnMeta> columns = new ArraySet<>(meta.getColumns());
        columns.retainAll(table.getEagerLoaded());
        if (table.isJoinTable())
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
        final Select statement = new QueryBuilder(this.session.getCluster())
                .select(names)
                .from(this.session.getKeyspace(), table.getName());
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

        // query results
        return this.session.executeAsync(statement, DatabaseActivity.Type.SELECT);
    }


    private static class KeyContext
    {
        private final EntityMeta entity;

        private final List<Row> rows = new ArrayList<>();


        private KeyContext(final EntityMeta meta)
        {
            this.entity = meta;
        }
    }
}
