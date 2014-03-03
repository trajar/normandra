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
    /**
     * close session, release any associated resources
     */
    void close();

    /**
     * clear session context and any associated cached data
     */
    void clear() throws NormandraException;

    /**
     * query entity discriminator based on key value
     */
    <T> Object discriminator(EntityMeta<T> meta, Object key) throws NormandraException;

    /**
     * save entity instance (update, insert)
     */
    <T> void save(EntityMeta<T> meta, T element) throws NormandraException;

    /**
     * remove entity instance (delete)
     */
    <T> void delete(EntityMeta<T> meta, T element) throws NormandraException;

    /**
     * being unit of work
     */
    void beginWork() throws NormandraException;

    /**
     * commit unit of work, executing any stored/batched operations
     */
    void commitWork() throws NormandraException;

    /**
     * rollback unit of work, clearing stored/batched operations
     */
    void rollbackWork() throws NormandraException;
}
