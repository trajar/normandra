package org.normandra;

import com.orientechnologies.common.concur.ONeedRetryException;
import org.apache.commons.lang.NullArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a transaction worker
 * <p>
 * User: bowen Date: 8/30/14
 */
public class Transaction implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(Transaction.class);

    private final Transactional sesssion;

    private final boolean ownsTransaction;

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
        if (Boolean.FALSE.equals(this.success))
        {
            logger.warn("Moving transaction from 'failure' to 'success'.  Please ensure this is desired state.");
        }
        this.success = Boolean.TRUE;
    }

    public void failure()
    {
        this.success = Boolean.FALSE;
    }

    public void execute(final TransactionRunnable worker) throws NormandraException
    {
        if (null == worker)
        {
            return;
        }

        for (int i = 1; i <= 10; i++)
        {
            try
            {
                worker.run(this);
                return;
            }
            catch (final ONeedRetryException e)
            {
                logger.info("Error executing unit of work, recovering from retry-exception [attempt #" + i + "].", e);
            }
            catch (final Exception e)
            {
                this.success = Boolean.FALSE;
                throw new NormandraException("Unable to execute unit of work.", e);
            }
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
