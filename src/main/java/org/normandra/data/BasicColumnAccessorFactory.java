package org.normandra.data;

import org.normandra.association.BasicElementFactory;
import org.normandra.meta.EntityContext;

import java.lang.reflect.Field;

/**
 * a factory that constructs accessors capable of using reflection and lazy-loaded collections
 * <p>
 * User: bowen
 * Date: 9/23/14
 */
public class BasicColumnAccessorFactory implements ColumnAccessorFactory
{
    @Override
    public ColumnAccessor createBasic(Field field, Class<?> clazz)
    {
        return new BasicColumnAccessor(field, clazz);
    }


    @Override
    public ColumnAccessor createManyJoin(Field field, EntityContext meta, boolean lazy, boolean mapped)
    {
        return new ManyJoinColumnAccessor(field, meta, lazy, mapped, new BasicElementFactory(meta));
    }


    @Override
    public ColumnAccessor createSingleJoin(Field field, EntityContext meta, boolean lazy)
    {
        return new SingleJoinColumnAccessor(field, meta, lazy, new BasicElementFactory(meta));
    }
}
