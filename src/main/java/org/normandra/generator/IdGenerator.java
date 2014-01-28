package org.normandra.generator;

import org.normandra.NormandraException;
import org.normandra.meta.EntityMeta;

/**
 * User: bowen
 * Date: 1/21/14
 */
public interface IdGenerator<T>
{
    T generate(EntityMeta entity) throws NormandraException;
}
