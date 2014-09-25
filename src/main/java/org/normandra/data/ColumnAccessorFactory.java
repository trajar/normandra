package org.normandra.data;

import org.normandra.meta.EntityContext;

import java.lang.reflect.Field;

/**
 * a factory to construct column accessor instances
 * <p>
 * User: bowen
 * Date: 9/23/14
 */
public interface ColumnAccessorFactory
{
    ColumnAccessor createBasic(Field field, Class<?> clazz);
    ColumnAccessor createManyJoin(Field field, EntityContext meta, boolean lazy);
    ColumnAccessor createSingleJoin(Field field, EntityContext meta, boolean lazy);
}
