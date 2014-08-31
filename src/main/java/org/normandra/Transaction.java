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

    private boolean ownsTransaction;

    private boolean success = false;


    public Transaction(final Transactional session) throws NormandraException
    {
        if (null == session)
        {
            throw new NullArgumentException("session");
        }
        this.sesssion = session;
        if (this.sesssion.pendingWork())
        {
            this.ownsTransaction = false;
        }
        else
        {
            this.ownsTransaction = true;
            this.sesssion.beginWork();
        }
    }


    public void success()
    {
        this.success = true;
    }


    public void failure()
    {
        this.success = false;
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
        if (this.success)
        {
            this.sesssion.commitWork();
        }
        else
        {
            this.sesssion.rollbackWork();
        }
    }
}
