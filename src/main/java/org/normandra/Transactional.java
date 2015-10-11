package org.normandra;

/**
 * a element capable of handling unit of work operations
 * <p>
 *  Date: 8/30/14
 */
public interface Transactional
{
    /**
     * close session, release any associated resources
     */
    void close();

    /**
     * executes a worker within a transaction context
     */
    void withTransaction(TransactionRunnable worker) throws NormandraException;

    /**
     * start transaction or unit of work
     *
     * @return Returns a transaction instance, which should be closed.
     */
    Transaction beginTransaction() throws NormandraException;

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
