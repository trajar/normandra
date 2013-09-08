package org.normandra.config;

import org.apache.commons.lang.NullArgumentException;

/**
 * collection column meta-data
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
public class CollectionMeta extends ColumnMeta
{
    private final Class<?> generic;


    public CollectionMeta(final String name, final Class<?> clazz, final Class<?> generic)
    {
        super(name, clazz, false);
        if (null == generic)
        {
            throw new NullArgumentException("generic");
        }
        this.generic = generic;
    }


    public Class<?> getGeneric()
    {
        return this.generic;
    }
}
