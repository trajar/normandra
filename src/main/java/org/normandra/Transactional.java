package org.normandra;

/**
 * a element capable of handling unit of work operations
 * <p>
 * User: bowen
 * Date: 8/30/14
 */
public interface Transactional
{
    /**
     * executes a worker within a transaction context
     */
    void withTransaction(Runnable worker) throws NormandraException;

    /**
     * start transaction or unit of work
     *
     * @return Returns a transaction instance, which should be closed.
     */
    Transaction beginTransaction();

    /**
     * @return Returns true if we have begun a unit of work (i.e. transaction).
     */
    boolean pendingWork();

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
