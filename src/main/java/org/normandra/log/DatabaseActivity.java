package org.normandra.log;

import java.util.Date;

/**
 * an instance of database listActivity
 * <p>
 * User: bowen
 * Date: 4/4/14
 */
public interface DatabaseActivity
{
    public static enum Type
    {
        SELECT, UPDATE, INSERT, DELETE, BATCH, ADMIN
    }

    Type getType();
    long getDuration();
    Date getDate();

    CharSequence getInformation();
}
