package org.normandra.data;

import org.normandra.meta.ColumnMeta;

import java.util.Collections;
import java.util.Map;

/**
 * an empty id accessor, used when we have multiple @Id annotations but outside an embeddable type
 * <p>
 * 
 * Date: 3/2/14
 */
public class NullIdAccessor implements IdAccessor
{
    private static final NullIdAccessor instance = new NullIdAccessor();


    public static IdAccessor getInstance()
    {
        return NullIdAccessor.instance;
    }


    private NullIdAccessor()
    {
    }


    @Override
    public Object fromEntity(Object entity)
    {
        return null;
    }


    @Override
    public Map<String, Object> fromKey(Object key)
    {
        return Collections.emptyMap();
    }


    @Override
    public Object fromData(Map<ColumnMeta, Object> data)
    {
        return null;
    }


    @Override
    public Object toKey(Map<String, Object> map)
    {
        return null;
    }
}
