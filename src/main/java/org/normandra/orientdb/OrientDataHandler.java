package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.normandra.data.DataHandler;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * simple utility class to help simplify saving of orientdb documents
 * <p>
 * Date: 6/4/14
 */
public class OrientDataHandler implements DataHandler
{
    private final OrientDatabaseSession session;

    private final List<ODocument> documents = new ArrayList<>();

    public OrientDataHandler(OrientDatabaseSession session)
    {
        this.session = session;
    }

    @Override
    public boolean save(final EntityMeta entity, final TableMeta table, final Map<ColumnMeta, Object> data)
    {
        final Map<String, Object> keymap = new TreeMap<>();
        for (final ColumnMeta column : table.getPrimaryKeys())
        {
            final String columnName = column.getName();
            final Object value = data.get(column);
            if (value != null)
            {
                keymap.put(columnName, value);
            }
        }

        final ODocument document;
        final OIdentifiable existing = this.session.findIdByMap(table, keymap);
        if (existing != null)
        {
            document = this.session.findDocument(existing);
        }
        else
        {
            final String schemaName = table.getName();
            document = this.session.getDatabase().newInstance(schemaName);
        }
        for (final Map.Entry<ColumnMeta, Object> entry : data.entrySet())
        {
            final ColumnMeta column = entry.getKey();
            final String name = column.getName();
            final Object value = entry.getValue();
            if (value != null)
            {
                final OType type = OrientUtils.columnType(column);
                final Object packed = OrientUtils.packValue(column, value);
                document.field(name, packed, type);
            }
            else
            {
                document.removeField(name);
            }
        }
        document.save();
        return this.documents.add(document);
    }

    @Override
    public boolean save(final EntityMeta entity, final TableMeta table, final Map<ColumnMeta, Object> keys, final ColumnMeta column, final Collection<?> items)
    {
        if (items.isEmpty())
        {
            // clear all items with key
            boolean updated = false;
            for (final ODocument document : this.findMatching(table, keys))
            {
                document.delete();
                updated = true;
            }
            return updated;
        }

        // get existing collection values
        final List<Object> removed = new ArrayList<>();
        for (final ODocument document : this.findMatching(table, keys))
        {
            final Object item = OrientUtils.unpackValue(document, column);
            if (item != null && !items.contains(item))
            {
                removed.add(item);
            }
        }

        // delete any removed items
        boolean updated = false;
        final Map<ColumnMeta, Object> datamap = new TreeMap<>(keys);
        for (final Object item : removed)
        {
            datamap.put(column, OrientUtils.packValue(column, item));
            for (final ODocument document : this.findMatching(table, datamap))
            {
                document.delete();
                updated = true;
            }
        }

        // build up key map
        final Map<String, Object> keymap = new HashMap<>(keys.size());
        for (final Map.Entry<ColumnMeta, Object> entry : keys.entrySet())
        {
            final String columnName = entry.getKey().getName();
            final Object value = entry.getValue();
            keymap.put(columnName, value);
        }

        // save new items
        for (final Object item : items)
        {
            // setup document
            keymap.put(column.getName(), item);
            final ODocument document;
            final OIdentifiable rid = this.session.findIdByMap(table, keymap);
            if (rid != null)
            {
                document = this.session.findDocument(rid);
            }
            else
            {
                final String schemaName = table.getName();
                document = this.session.getDatabase().newInstance(schemaName);
            }

            // save primary keys
            for (final Map.Entry<ColumnMeta, Object> entry : keys.entrySet())
            {
                final ColumnMeta key = entry.getKey();
                final Object value = entry.getValue();
                final String name = key.getName();
                final OType type = OrientUtils.columnType(key);
                final Object packed = OrientUtils.packValue(key, value);
                document.field(name, packed, type);
            }

            // save column value
            final String name = column.getName();
            final OType type = OrientUtils.columnType(column);
            final Object packed = OrientUtils.packValue(column, item);
            document.field(name, packed, type);
            document.save();
            updated |= this.documents.add(document);
        }
        return updated;
    }

    private Collection<ODocument> findMatching(final TableMeta table, final Map<ColumnMeta, Object> keys)
    {
        final List<Object> parameters = new ArrayList<>(keys.size());
        final StringBuilder query = new StringBuilder()
            .append("SELECT FROM ").append(table.getName()).append(" ")
            .append("WHERE ");
        for (final Map.Entry<ColumnMeta, Object> entry : keys.entrySet())
        {
            final ColumnMeta column = entry.getKey();
            if (!parameters.isEmpty())
            {
                query.append(" AND ");
            }
            query.append(column.getProperty()).append(" = ?");
            parameters.add(entry.getValue());
        }

        if (parameters.isEmpty())
        {
            return Collections.emptyList();
        }

        return this.session.query(query.toString(), parameters);
    }
}
