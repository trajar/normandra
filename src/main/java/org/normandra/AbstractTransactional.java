package org.normandra;

/**
 * a base transactional class
 * <p>
 * 
 * Date: 8/31/14
 */
abstract public class AbstractTransactional implements Transactional
{
    @Override
    public void withTransaction(final TransactionRunnable worker) throws NormandraException
    {
        try (final Transaction tx = this.beginTransaction())
        {
            tx.execute(worker);
        }
        catch (final Exception e)
        {
            throw new NormandraException("Unable to execute transaction.", e);
        }
    }


    @Override
    public Transaction beginTransaction() throws NormandraException
    {
        return new Transaction(this);
    }
}
