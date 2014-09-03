package org.normandra.data;

import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.JoinCollectionMeta;
import org.normandra.meta.JoinColumnMeta;
import org.normandra.meta.MappedColumnMeta;
import org.normandra.meta.TableMeta;

/**
 * a simple data holder factory
 * <p/>
 * User: bowen
 * Date: 5/15/14
 */
public interface DataHolderFactory
{
    DataHolder createStatic(Object value);
    DataHolder createLazy(EntityMeta meta, TableMeta table, ColumnMeta column, Object key);
    DataHolder createJoinCollection(EntityMeta meta, TableMeta table, JoinCollectionMeta column, Object key);
    DataHolder createJoinColumn(EntityMeta meta, TableMeta table, JoinColumnMeta column, Object key);
    DataHolder createMappedColumn(EntityMeta meta, MappedColumnMeta column, Object key);
}
