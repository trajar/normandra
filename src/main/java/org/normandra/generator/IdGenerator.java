package org.normandra.generator;

import org.normandra.EntitySession;
import org.normandra.NormandraException;
import org.normandra.meta.EntityMeta;

import java.io.Serializable;

/**
 * id generator sequence api
 */
public interface IdGenerator
{
    Serializable generate(EntitySession session, EntityMeta entity) throws NormandraException;
}
