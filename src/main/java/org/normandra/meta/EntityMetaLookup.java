package org.normandra.meta;

import java.util.Collection;
import java.util.List;

/**
 * a lookup for handling entity meta
 * <p>
 * User: bowen
 * Date: 7/23/14
 */
public interface EntityMetaLookup
{
    EntityMeta getMeta(Class<?> clazz);
    EntityMeta getMeta(String labelOrType);
    List<EntityMeta> findMeta(Class<?> clazz);
    EntityContext findContext(Class<?> clazz);
    boolean contains(Class<?> clazz);
    boolean contains(EntityMeta meta);
    Collection<EntityMeta> list();
    int size();
}
