package org.normandra.data;

import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.TableMeta;

import java.util.Collection;
import java.util.Map;

/**
 * a simple interface capable of saving entities to database
 * <p/>
 * User: bowen
 * Date: 5/25/14
 */
public interface DataHandler
{
    boolean save(EntityMeta entity, TableMeta table, Map<ColumnMeta, Object> data);
    boolean save(EntityMeta entity, TableMeta table, Map<ColumnMeta, Object> keys, ColumnMeta column, Collection<?> items);
}
