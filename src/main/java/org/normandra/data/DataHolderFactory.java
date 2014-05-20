package org.normandra.data;

import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityMeta;
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
}
