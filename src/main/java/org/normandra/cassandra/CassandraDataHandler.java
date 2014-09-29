package org.normandra.cassandra;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.commons.lang.NullArgumentException;
import org.normandra.data.DataHandler;
import org.normandra.log.DatabaseActivity;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * a cassandra data handler, used in coordination with entity helper to standardize save/delete operations
 * <p>
 * <p>
 * User: bowen
 * Date: 5/27/14
 */
public class CassandraDataHandler implements DataHandler
{
    private final CassandraDatabaseSession session;

    private final List<RegularStatement> operations = new ArrayList<>();


    public CassandraDataHandler(final CassandraDatabaseSession session)
    {
        if (null == session)
        {
            throw new NullArgumentException("session");
        }
        this.session = session;
    }


    public List<RegularStatement> getOperations()
    {
        return Collections.unmodifiableList(this.operations);
    }


    @Override
    public boolean save(final EntityMeta entity, final TableMeta table, final Map<ColumnMeta, Object> data)
    {
        if (data.isEmpty())
        {
            return false;
        }

        boolean hasInsert = false;
        final String keyspaceName = this.session.getKeyspace();
        Insert statement = QueryBuilder.insertInto(keyspaceName, table.getName());
        for (final Map.Entry<ColumnMeta, Object> entry : data.entrySet())
        {
            final ColumnMeta column = entry.getKey();
            final Object value = entry.getValue();
            if (value != null)
            {
                hasInsert = true;
                statement = statement.value(column.getName(), value);
            }
            else
            {
                boolean hasWhere = false;
                Delete delete = QueryBuilder.delete(column.getName()).from(keyspaceName, table.getName());
                for (final ColumnMeta key : table.getPrimaryKeys())
                {
                    final Object keyValue = data.get(key);
                    if (keyValue != null)
                    {
                        hasWhere = true;
                        delete.where(QueryBuilder.eq(key.getName(), keyValue));
                    }
                }
                if (hasWhere)
                {
                    this.operations.add(delete);
                }
            }
        }
        if (hasInsert)
        {
            this.operations.add(statement);
            return true;
        }
        else
        {
            return false;
        }
    }


    @Override
    public boolean save(final EntityMeta entity, final TableMeta table, final Map<ColumnMeta, Object> keys, final ColumnMeta column, final Collection<?> items)
    {
        if (keys.isEmpty())
        {
            return false;
        }

        final String keyspaceName = this.session.getKeyspace();

        if (items.isEmpty())
        {
            // clear all items with key
            boolean hasWhere = false;
            Delete delete = QueryBuilder.delete().all().from(keyspaceName, table.getName());
            for (final ColumnMeta key : table.getPrimaryKeys())
            {
                final Object value = keys.get(key);
                if (value != null)
                {
                    hasWhere = true;
                    delete.where(QueryBuilder.eq(key.getName(), value));
                }
            }
            if (hasWhere)
            {
                this.operations.add(delete);
                return true;
            }
        }

        // get existing collection values
        Select query = QueryBuilder.select(column.getName()).from(keyspaceName, table.getName());
        for (final Map.Entry<ColumnMeta, Object> entry : keys.entrySet())
        {
            final ColumnMeta key = entry.getKey();
            final Object value = entry.getValue();
            if (value != null)
            {
                query.where(QueryBuilder.eq(key.getName(), value));
            }
        }
        final List<Row> rows = this.session.executeSync(query, DatabaseActivity.Type.SELECT).all();
        final List<Object> removed = new ArrayList<>(rows.size());
        if (!rows.isEmpty())
        {
            final Set<Object> set = new HashSet<>(items);
            for (final Row row : rows)
            {
                try
                {
                    final Object item = CassandraUtils.unpackValue(row, column);
                    if (item != null && !set.contains(item))
                    {
                        removed.add(item);
                    }
                }
                catch (final Exception e)
                {
                    throw new IllegalStateException("Unable to toEntity row [" + row + "] for collection column [" + column + "].", e);
                }
            }
        }

        // remove any unused values
        boolean updated = false;
        for (final Object item : removed)
        {
            boolean hasWhere = false;
            Delete delete = QueryBuilder.delete().all().from(keyspaceName, table.getName());
            for (final ColumnMeta key : table.getPrimaryKeys())
            {
                final Object value = keys.get(key);
                if (value != null)
                {
                    hasWhere = true;
                    delete.where(QueryBuilder.eq(key.getName(), value));
                }
            }
            delete.where(QueryBuilder.eq(column.getName(), item));
            if (hasWhere)
            {
                updated |= this.operations.add(delete);
            }
        }

        // save existing or new values
        for (final Object item : items)
        {
            boolean hasInsert = false;
            Insert statement = QueryBuilder.insertInto(keyspaceName, table.getName());
            for (final Map.Entry<ColumnMeta, Object> entry : keys.entrySet())
            {
                final ColumnMeta key = entry.getKey();
                final Object value = entry.getValue();
                if (value != null)
                {
                    hasInsert = true;
                    statement = statement.value(key.getName(), value);
                }
            }
            if (hasInsert)
            {
                statement.value(column.getName(), item);
                updated |= this.operations.add(statement);
            }
        }
        return updated;
    }
}