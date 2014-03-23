package org.normandra.generator;

import org.normandra.NormandraException;
import org.normandra.meta.EntityMeta;

import java.io.Serializable;

/**
 * User: bowen
 * Date: 1/21/14
 */
public interface IdGenerator
{
    Serializable generate(EntityMeta entity) throws NormandraException;
}
