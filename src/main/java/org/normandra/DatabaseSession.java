package org.normandra;

import org.normandra.meta.EntityMeta;

/**
 * a database session
 * <p/>
 * User: bowen
 * Date: 2/1/14
 */
public interface DatabaseSession extends EntitySession
{
    void close();
    void clear() throws NormandraException;
    void beginTransaction() throws NormandraException;
    void commitTransaction() throws NormandraException;
    void rollbackTransaction() throws NormandraException;
    <T> Object discriminator(EntityMeta<T> meta, Object key) throws NormandraException;
    <T> void save(EntityMeta<T> meta, T element) throws NormandraException;
    <T> void delete(EntityMeta<T> meta, T element) throws NormandraException;
}
