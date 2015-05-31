package org.normandra;

/**
 * a runnable worker within a transaction
 * <p>
 * User: bowen
 * Date: 5/30/15
 */
public interface TransactionRunnable
{
    void run(Transaction tx) throws Exception;
}
