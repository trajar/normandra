package org.normandra.meta;

import org.normandra.data.IdAccessor;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * an abstraction of entity meta information, which can be backed by a single entity or hierarchy
 * <p>
 * User: bowen
 * Date: 3/30/14
 */
public interface EntityContext
{
    IdAccessor getId();
    Collection<EntityMeta> getEntities();
    Set<TableMeta> getTables();
    Set<TableMeta> getPrimaryTables();
    Set<TableMeta> getSecondaryTables();
    ColumnMeta getPrimaryKey();
    Set<ColumnMeta> getPrimaryKeys();
    Set<ColumnMeta> getColumns();
    EntityMeta findEntity(Map<ColumnMeta, Object> data);
    TableMeta findTable(String name);
}
