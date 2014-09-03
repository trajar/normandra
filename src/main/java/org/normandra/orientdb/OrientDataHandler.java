package org.normandra.orientdb;

import com.orientechnologies.orient.core.id.ORID;
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
 * User: bowen
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


    public Collection<ODocument> getDocuments()
    {
        return Collections.unmodifiableCollection(this.documents);
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
        final ORID existing = this.session.findIdByMap(entity, table, keymap);
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
            final Object value = entry.getValue();
            if (!column.isVirtual())
            {
                if (value != null)
                {
                    final String name = column.getName();
                    final OType type = OrientUtils.columnType(column);
                    final Object packed = OrientUtils.packRaw(column, value);
                    document.field(name, packed, type);
                }
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
            for (final ODocument document : this.findDocuments(entity, table, keys))
            {
                document.delete();
                updated = true;
            }
            return updated;
        }

        // get existing collection values
        final Iterable<ODocument> documents = this.findDocuments(entity, table, keys);
        final List<Object> removed = new ArrayList<>();
        for (final ODocument document : documents)
        {
            final Object item = OrientUtils.unpackValue(document, column);
            if (item != null)
            {
                if (!items.contains(item))
                {
                    removed.add(item);
                }
            }
        }

        // delete any removed items
        boolean updated = false;
        final Map<ColumnMeta, Object> datamap = new TreeMap<>(keys);
        for (final Object item : removed)
        {
            // clear all items with key
            datamap.put(column, item);
            for (final ODocument document : this.findDocuments(entity, table, datamap))
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
            final ORID rid = this.session.findIdByMap(entity, table, keymap);
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
                final Object packed = OrientUtils.packRaw(key, value);
                document.field(name, packed, type);
            }
            // save column value
            final String name = column.getName();
            final OType type = OrientUtils.columnType(column);
            final Object packed = OrientUtils.packRaw(column, item);
            document.field(name, packed, type);
            document.save();
            updated |= this.documents.add(document);
        }
        return updated;
    }


    private Iterable<ODocument> findDocuments(final EntityMeta entity, final TableMeta table, final Map<ColumnMeta, Object> keys)
    {
        final List<Object> parameters = new ArrayList<>(keys.size());
        final StringBuilder query = new StringBuilder();
        query.append("select from ").append(table.getName());
        query.append(" where ");
        boolean first = true;
        for (final Map.Entry<ColumnMeta, Object> entry : keys.entrySet())
        {
            final String columnName = entry.getKey().getName();
            final Object value = OrientUtils.packRaw(entry.getKey(), entry.getValue());
            if (!first)
            {
                query.append(" and ");
            }
            query.append(columnName).append(" = ?");
            parameters.add(value);
            first = false;
        }
        return this.session.query(query.toString(), parameters);
    }
}
