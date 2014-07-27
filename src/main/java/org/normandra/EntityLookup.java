package org.normandra;

import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;

import java.util.List;

/**
 * a lookup for handling entity meta
 * <p>
 * User: bowen
 * Date: 7/23/14
 */
public interface EntityLookup
{
    EntityMeta getMeta(Class<?> clazz);
    EntityMeta getMeta(String labelOrType);
    List<EntityMeta> findMeta(Class<?> clazz);
    EntityContext findContext(Class<?> clazz);
}
