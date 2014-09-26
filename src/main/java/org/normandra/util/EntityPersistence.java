package org.normandra.util;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.EntitySession;
import org.normandra.NormandraException;
import org.normandra.data.BasicDataHolder;
import org.normandra.data.ColumnAccessor;
import org.normandra.data.DataHandler;
import org.normandra.data.DataHolder;
import org.normandra.generator.IdGenerator;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.SingleEntityContext;
import org.normandra.meta.TableMeta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * basic entity handler api
 * <p>
 * User: bowen
 * Date: 5/25/14
 */
public class EntityPersistence
{
    private final EntitySession session;


    public EntityPersistence(final EntitySession session)
    {
        if (null == session)
        {
            throw new NullArgumentException("session");
        }
        this.session = session;
    }


    public void save(final EntityMeta entity, final Object instance, final DataHandler handler) throws NormandraException
    {
        // generate any primary ids
        for (final TableMeta table : entity)
        {
            for (final ColumnMeta column : table.getColumns())
            {
                final ColumnAccessor accessor = entity.getAccessor(column);
                final IdGenerator generator = entity.getGenerator(column);
                if (generator != null && accessor != null && accessor.isEmpty(instance))
                {
                    final Object generated = generator.generate(entity);
                    final DataHolder data = new BasicDataHolder(generated);
                    accessor.setValue(instance, data, this.session);
                }
            }
        }

        // handle data for each entity
        for (final TableMeta table : entity)
        {
            if (table.isJoinTable())
            {
                // join columns usually have an extra primary key (indexed)
                // handle each extra key as separate row
                final List<ColumnMeta> collections = new ArrayList<>(table.getColumns().size());
                for (final ColumnMeta column : table.getColumns())
                {
                    if (column.isCollection())
                    {
                        collections.add(column);
                    }
                }
                if (collections.isEmpty())
                {
                    // this is a join table, but without collections
                    handler.save(entity, table, this.mapData(entity, table, instance));
                }
                else
                {
                    // delete existing keys prior to rebuilding join columns
                    final Map<ColumnMeta, Object> keys = new TreeMap<>();
                    for (final ColumnMeta keyColumn : new SingleEntityContext(entity).getPrimaryKeys())
                    {
                        final ColumnAccessor keyAccessor = entity.getAccessor(keyColumn);
                        final Object keyValue = keyAccessor != null ? keyAccessor.getValue(instance, session) : null;
                        if (keyValue != null)
                        {
                            keys.put(keyColumn, keyValue);
                        }
                    }

                    // we will need to create a new entry for each collection item
                    for (final ColumnMeta column : collections)
                    {
                        final ColumnAccessor accessor = entity.getAccessor(column);
                        if (accessor != null && accessor.isLoaded(instance))
                        {
                            final Object value = accessor.getValue(instance, session);
                            final Collection list = value != null ? (Collection) value : Collections.emptyList();
                            handler.save(entity, table, keys, column, list);
                        }
                    }
                }
            }
            else
            {
                // regular table
                final Map<ColumnMeta, Object> data = this.mapData(entity, table, instance);
                handler.save(entity, table, data);
            }
        }
    }


    private Map<ColumnMeta, Object> mapData(final EntityMeta entity, final TableMeta table, final Object instance) throws NormandraException
    {
        final Map<ColumnMeta, Object> data = new TreeMap<>();
        for (final ColumnMeta column : table)
        {
            final ColumnAccessor accessor = entity.getAccessor(column);
            if (accessor != null && accessor.isLoaded(instance))
            {
                final Object value = accessor.getValue(instance, session);
                if (value != null)
                {
                    data.put(column, value);
                }
            }
        }
        return data;
    }
}
