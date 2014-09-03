package org.normandra.orientdb;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.commons.lang.StringUtils;
import org.normandra.data.BasicDataHolder;
import org.normandra.data.DataHolder;
import org.normandra.data.DataHolderFactory;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.DiscriminatorMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.JoinCollectionMeta;
import org.normandra.meta.JoinColumnMeta;
import org.normandra.meta.MappedColumnMeta;
import org.normandra.meta.SingleEntityContext;
import org.normandra.meta.TableMeta;
import org.normandra.util.ArraySet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * orientdb data holder factory
 * <p>
 * User: bowen
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
        final Map<String, Object> keymap = entity.getId().fromKey(key);
        if (null == keymap || keymap.isEmpty())
        {
            return null;
        }
        return new OrientLazyDataHolder(this.session, entity, table, column, keymap);
    }


    @Override
    public DataHolder createJoinCollection(final EntityMeta entity, final TableMeta table, final JoinCollectionMeta column, Object key)
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
        final OrientDocumentHandler handler = new OrientDocumentHandler()
        {
            @Override
            public Object convert(ODocument document)
            {
                return OrientUtils.unpackValue(document, column);
            }
        };
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
        final OrientDocumentHandler handler = new OrientDocumentHandler()
        {
            @Override
            public Object convert(ODocument document)
            {
                return OrientUtils.unpackKey(new SingleEntityContext(entity), document);
            }
        };
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

        final Set<String> columnNames = new ArraySet<>();
        for (final ColumnMeta mappedKey : mappedEntity.getPrimaryKeys())
        {
            columnNames.add(mappedKey.getName());
        }
        for (final EntityMeta meta : mappedEntity.getEntities())
        {
            final DiscriminatorMeta descrim = meta.getDiscriminator();
            if (descrim != null)
            {
                columnNames.add(descrim.getColumn().getName());
            }
        }

        final StringBuilder query = new StringBuilder();
        query.append("select ").append(StringUtils.join(columnNames, ", ")).append(" from ").append(mappedTable.getName());
        query.append(" where ").append(mappedColumn.getName()).append(" = ?");
        final OrientDocumentHandler handler = new OrientDocumentHandler()
        {
            @Override
            public Object convert(ODocument document)
            {
                return OrientUtils.unpackKey(mappedEntity, document);
            }
        };
        return new OrientLazyQueryHolder(this.session, mappedEntity, mappedTable, column.isCollection(), query.toString(), Arrays.asList(key), handler);
    }
}
