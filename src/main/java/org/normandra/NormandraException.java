package org.normandra;

/**
 * base normandra exception
 * <p/>
 * User: bowen
 * Date: 1/15/14
 */
public class NormandraException extends Exception
{
    public NormandraException(final String msg)
    {
        super(msg);
    }


    public NormandraException(final String msg, final Exception e)
    {
        super(msg, e);
    }
}
