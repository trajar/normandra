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

    private Boolean success = null;


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
        this.success = Boolean.TRUE;
    }


    public void failure()
    {
        this.success = Boolean.FALSE;
    }


    public boolean execute(final Runnable worker) throws NormandraException
    {
        if (null == worker)
        {
            return false;
        }

        if (Boolean.FALSE.equals(this.success))
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
            this.success = Boolean.TRUE;
            return true;
        }
        catch (final Exception e)
        {
            this.success = Boolean.FALSE;
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
        if (Boolean.TRUE.equals(this.success))
        {
            this.sesssion.commitWork();
        }
        else
        {
            this.sesssion.rollbackWork();
        }
    }
}
