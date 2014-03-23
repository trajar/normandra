package org.normandra.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.normandra.NormandraException;
import org.normandra.cache.EntityCache;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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


    public Object query(final EntityMeta meta, final Object key) throws NormandraException
    {
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
            // pull primary tables necessary
            final Map<TableMeta, Row> rows = new TreeMap<>();
            for (final TableMeta table : meta.getPrimaryTables())
            {
                final ResultSet results = this.buildGet(meta, table, key);
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
            if (!CassandraUtils.update(meta, entity, rows, this.session))
            {
                return null;
            }
            this.cache.put(meta, entity);

            // now pull secondary tables
            for (final TableMeta table : meta.getSecondaryTables())
            {
                final ResultSet results = this.buildGet(meta, table, key);
                if (results != null)
                {
                    CassandraUtils.update(meta, entity, table, results, this.session);
                }
            }

            // done
            return entity;
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get entity [" + meta + "] by key [" + key + "].", e);
        }
    }


    public List<Object> query(final EntityMeta meta, final Object... keys) throws NormandraException
    {
        if (null == meta || null == keys || keys.length <= 0)
        {
            return null;
        }

        final List<Object> entities = new ArrayList<>(keys.length);
        for (final Object key : keys)
        {
            if (key instanceof Serializable)
            {
                // check cache
                final Object existing = this.cache.get(meta, (Serializable) key);
                if (existing != null)
                {
                    entities.add(existing);
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
                final ResultSet results = this.buildGet(meta, table, keys);
                if (results != null)
                {
                    for (final Row row : results)
                    {
                        final Object key = CassandraUtils.key(meta, table, row);
                        if (key != null)
                        {
                            KeyContext ctx = keymap.get(key);
                            if (null == ctx)
                            {
                                ctx = new KeyContext(table);
                                keymap.put(key, ctx);
                            }
                            ctx.rows.add(row);
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
                final Object entity = meta.getType().newInstance();
                for (final Row row : rows)
                {
                    CassandraUtils.update(meta, entity, table, row, this.session);
                }
                this.cache.put(meta, entity);
                entities.add(entity);
            }
            return Collections.unmodifiableList(entities);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to get entity [" + meta + "] by keys [" + keys + "].", e);
        }
    }


    private ResultSet buildGet(final EntityMeta meta, final TableMeta table, final Object... keys) throws NormandraException
    {
        // get columns to query
        final List<ColumnMeta> columns = new ArrayList<>(table.getColumns());
        final String[] names = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++)
        {
            names[i] = columns.get(i).getName();
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
        private final TableMeta table;

        private final List<Row> rows = new ArrayList<>();


        private KeyContext(final TableMeta table)
        {
            this.table = table;
        }
    }
}
