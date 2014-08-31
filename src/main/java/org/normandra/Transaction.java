package org.normandra;

import org.apache.commons.lang.NullArgumentException;

/**
 * a transaction worker
 * <p>
 * User: bowen
 * Date: 8/30/14
 */
public class Transaction implements AutoCloseable
{
    private final Transactional sesssion;

    private boolean ownsTransaction = false;

    private int numOperations = 0;

    private boolean success = true;


    public Transaction(final Transactional session)
    {
        if (null == session)
        {
            throw new NullArgumentException("session");
        }
        this.sesssion = session;
    }


    public boolean execute(final Runnable worker) throws NormandraException
    {
        if (null == worker)
        {
            return false;
        }
        if (!this.success)
        {
            return false;
        }

        if (!this.ownsTransaction && !this.sesssion.pendingWork())
        {
            this.ownsTransaction = true;
            this.sesssion.beginWork();
        }

        try
        {
            worker.run();
            this.numOperations++;
            return true;
        }
        catch (final Exception e)
        {
            this.success = false;
            throw new NormandraException("Unable to execute unit of work.", e);
        }
    }


    @Override
    public void close() throws Exception
    {
        if (!this.ownsTransaction)
        {
            return;
        }
        if (this.numOperations <= 0 || !this.success)
        {
            this.sesssion.rollbackWork();
        }
        else
        {
            this.sesssion.commitWork();
        }
    }
}
