package org.normandra.orientdb;

import org.normandra.data.BasicDataHolder;
import org.normandra.data.DataHolder;
import org.normandra.data.DataHolderFactory;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.JoinCollectionMeta;
import org.normandra.meta.JoinColumnMeta;
import org.normandra.meta.MappedColumnMeta;
import org.normandra.meta.SingleEntityContext;
import org.normandra.meta.TableMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * orientdb data holder factory
 * <p>
 * <p>
 * Date: 6/1/14
 */
public class OrientDataFactory implements DataHolderFactory
{
    private final OrientDatabaseSession session;

    public OrientDataFactory(OrientDatabaseSession session)
    {
        this.session = session;
    }

    @Override
    public DataHolder createStatic(Object value)
    {
        return new BasicDataHolder(value);
    }

    @Override
    public DataHolder createLazy(final EntityMeta entity, final TableMeta table, final ColumnMeta column, final Object key)
    {
        if (null == entity || null == table || null == column || null == key)
        {
            return null;
        }
        return new OrientLazyDataHolder(this.session, new SingleEntityContext(entity), column, key);
    }

    @Override
    public DataHolder createJoinCollection(final EntityMeta entity, final TableMeta table, final JoinCollectionMeta column, final Object key)
    {
        if (null == entity || null == table || null == column || null == key)
        {
            return null;
        }

        final Map<String, Object> keymap = entity.getId().fromKey(key);
        if (null == keymap || keymap.isEmpty())
        {
            return null;
        }

        final List<Object> parameters = new ArrayList<>(keymap.size());
        final StringBuilder query = new StringBuilder();
        query.append("select ").append(column.getName()).append(" from ").append(table.getName()).append(" where ");
        boolean first = true;
        for (final Map.Entry<String, Object> entry : keymap.entrySet())
        {
            if (!first)
            {
                query.append(" and ");
            }
            final String columnName = entry.getKey();
            final Object value = entry.getValue();
            query.append(columnName).append(" = ?");
            first = false;
            parameters.add(value);
        }
        final OrientDocumentHandler handler = document -> OrientUtils.unpackValue(document, column);
        return new OrientLazyQueryHolder(this.session, new SingleEntityContext(entity), table, column.isCollection(), query.toString(), parameters, handler);
    }

    @Override
    public DataHolder createJoinColumn(final EntityMeta entity, final TableMeta table, final JoinColumnMeta column, final Object key)
    {
        if (null == entity || null == table || null == column || null == key)
        {
            return null;
        }

        final Map<String, Object> keymap = entity.getId().fromKey(key);
        if (null == keymap || keymap.isEmpty())
        {
            return null;
        }

        final List<Object> parameters = new ArrayList<>(keymap.size());
        final StringBuilder query = new StringBuilder();
        query.append("select from ").append(table.getName()).append(" where ");
        boolean first = true;
        for (final Map.Entry<String, Object> entry : keymap.entrySet())
        {
            if (!first)
            {
                query.append(" and ");
            }
            final String columnName = entry.getKey();
            final Object value = entry.getValue();
            query.append(columnName).append(" = ?");
            first = false;
            parameters.add(value);
        }
        final OrientDocumentHandler handler = document -> OrientUtils.unpackKey(entity, document);
        return new OrientLazyQueryHolder(this.session, new SingleEntityContext(entity), table, column.isCollection(), query.toString(), parameters, handler);
    }

    @Override
    public DataHolder createMappedColumn(final EntityMeta entity, final MappedColumnMeta column, final Object key)
    {
        if (null == entity || null == column || null == key)
        {
            return null;
        }

        final EntityContext mappedEntity = column.getEntity();
        final TableMeta mappedTable = column.getTable();
        final ColumnMeta mappedColumn = column.getColumn();

        final String query = new StringBuilder()
            .append("select from ").append(mappedTable.getName()).append(" ")
            .append("where ").append(mappedColumn.getName()).append(" = ?")
            .toString();
        final OrientDocumentHandler handler = document -> OrientUtils.unpackKey(mappedEntity, document);
        final Object rid = this.session.findIdByKey(new SingleEntityContext(entity), key);
        return new OrientLazyQueryHolder(this.session, mappedEntity, mappedTable, column.isCollection(), query, Collections.singletonList(rid), handler);
    }
}
