package org.normandra;

/**
 * a base transactional class
 * <p>
 * User: bowen
 * Date: 8/31/14
 */
abstract public class AbstractTransactional implements Transactional
{
    @Override
    public void withTransaction(Runnable worker) throws NormandraException
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
    public Transaction beginTransaction()
    {
        return new Transaction(this);
    }
}
