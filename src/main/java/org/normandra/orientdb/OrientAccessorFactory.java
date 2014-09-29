package org.normandra.orientdb;

import org.normandra.data.BasicColumnAccessor;
import org.normandra.data.ColumnAccessor;
import org.normandra.data.ColumnAccessorFactory;
import org.normandra.data.ManyJoinColumnAccessor;
import org.normandra.data.SingleJoinColumnAccessor;
import org.normandra.meta.EntityContext;

import java.lang.reflect.Field;

/**
 * a factory using orientdb's graph-relation properties for single/multi-joined columns
 * <p>
 * User: bowen
 * Date: 9/23/14
 */
public class OrientAccessorFactory implements ColumnAccessorFactory
{
    @Override
    public ColumnAccessor createBasic(Field field, Class<?> clazz)
    {
        return new BasicColumnAccessor(field, clazz);
    }


    @Override
    public ColumnAccessor createManyJoin(Field field, EntityContext meta, boolean lazy)
    {
        return new ManyJoinColumnAccessor(field, meta, lazy, new OrientElementIdentity(meta));
    }


    @Override
    public ColumnAccessor createSingleJoin(Field field, EntityContext meta, boolean lazy)
    {
        return new SingleJoinColumnAccessor(field, meta, lazy, new OrientElementIdentity(meta));
    }
}
