package org.normandra.cassandra;

import org.normandra.data.BasicDataHolder;
import org.normandra.data.DataHolder;
import org.normandra.data.DataHolderFactory;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
import org.normandra.meta.JoinCollectionMeta;
import org.normandra.meta.JoinColumnMeta;
import org.normandra.meta.MappedColumnMeta;
import org.normandra.meta.TableMeta;

import java.util.Map;
import java.util.TreeMap;

/**
 * a cassandra data holder factory
 * <p/>
 * User: bowen
 * Date: 6/1/14
 */
public class CassandraDataFactory implements DataHolderFactory
{
    private final CassandraDatabaseSession session;


    public CassandraDataFactory(CassandraDatabaseSession session)
    {
        this.session = session;
    }


    @Override
    public DataHolder createStatic(final Object value)
    {
        return new BasicDataHolder(value);
    }


    @Override
    public DataHolder createLazy(final EntityContext entity, final TableMeta table, final ColumnMeta column, final Object key)
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
        return new CassandraLazyColumnHolder(this.session, entity, table, column, keymap);
    }


    @Override
    public DataHolder createJoinCollection(final EntityContext entity, final TableMeta table, final JoinCollectionMeta column, final Object key)
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
        return new CassandraLazyColumnHolder(session, entity, table, column, keymap);
    }


    @Override
    public DataHolder createJoinColumn(final EntityContext entity, final TableMeta table, final JoinColumnMeta column, final Object key)
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
        return new CassandraLazyKeyHolder(session, entity, table, column.isCollection(), keymap);
    }


    @Override
    public DataHolder createMappedColumn(final EntityContext entity, final MappedColumnMeta column, final Object key)
    {
        final EntityContext mappedEntity = column.getEntity();
        final TableMeta mappedTable = column.getTable();
        final ColumnMeta mappedColumn = column.getColumn();
        final Map<String, Object> datamap = new TreeMap<>();
        datamap.put(mappedColumn.getName(), key);
        return new CassandraLazyKeyHolder(this.session, mappedEntity, mappedTable, column.isCollection(), datamap);
    }
}
