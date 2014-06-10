package org.normandra.data;

import java.util.Map;

/**
 * an object capable of retrieving an entity's id as a single object (either via simple column or embeddable type)
 * <p/>
 * User: bowen
 * Date: 1/15/14
 */
public interface IdAccessor
{
    Object fromEntity(Object entity);
    Map<String, Object> fromKey(Object key);
    Object toKey(Map<String, Object> map);
}
