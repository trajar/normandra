package org.normandra;

import org.normandra.log.DatabaseActivity;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;

import java.util.List;
import java.util.Map;

/**
 * a database session
 * <p>
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
    void clear();

    /**
     * save entity instance (updateInstance, insert)
     */
    void save(EntityMeta meta, Object element) throws NormandraException;

    /**
     * remove entity instance (delete)
     */
    void delete(EntityMeta meta, Object element) throws NormandraException;

    /**
     * query database using string with mapped parameters
     */
    DatabaseQuery query(EntityContext meta, String query, Map<String, Object> parameters) throws NormandraException;

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

    /**
     * provides a set of listActivity from this session
     */
    List<DatabaseActivity> listActivity();
}