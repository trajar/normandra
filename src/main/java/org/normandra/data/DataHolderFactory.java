package org.normandra.data;

import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityContext;
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
    DataHolder createLazy(EntityContext meta, TableMeta table, ColumnMeta column, Object key);
    DataHolder createJoinCollection(EntityContext meta, TableMeta table, JoinCollectionMeta column, Object key);
    DataHolder createJoinColumn(EntityContext meta, TableMeta table, JoinColumnMeta column, Object key);
    DataHolder createMappedColumn(EntityContext meta, MappedColumnMeta column, Object key);
}
