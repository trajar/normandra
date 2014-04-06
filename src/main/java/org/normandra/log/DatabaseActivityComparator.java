package org.normandra.log;

import java.util.Comparator;

/**
 * a database listActivity comparator
 * <p>
 * User: bowen
 * Date: 4/4/14
 */
public class DatabaseActivityComparator implements Comparator<DatabaseActivity>
{
    private static final DatabaseActivityComparator instance = new DatabaseActivityComparator();


    public static Comparator<DatabaseActivity> getInstance()
    {
        return DatabaseActivityComparator.instance;
    }


    private DatabaseActivityComparator()
    {
    }


    @Override
    public int compare(final DatabaseActivity left, final DatabaseActivity right)
    {
        if (null == left && null == right)
        {
            return 0;
        }
        if (null == left)
        {
            return -1;
        }
        if (null == right)
        {
            return 1;
        }
        return left.getDate().compareTo(right.getDate());
    }
}
