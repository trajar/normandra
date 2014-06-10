package org.normandra.orientdb;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.log.DatabaseActivity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * a orientdb database index lookup activity
 * <p/>
 * User: bowen
 * Date: 4/4/14
 */
public class OrientIndexActivity implements DatabaseActivity
{
    private final Type type;

    private final String index;

    private final List<Object> keys;

    private final Date date;

    private final long duration;


    public OrientIndexActivity(Type type, String index, Collection<?> keys, Date date, long duration)
    {
        if (null == type)
        {
            throw new NullArgumentException("type");
        }
        if (null == index)
        {
            throw new NullArgumentException("index");
        }
        if (null == keys)
        {
            throw new NullArgumentException("keys");
        }
        if (null == date)
        {
            throw new NullArgumentException("date");
        }
        this.type = type;
        this.index = index;
        this.keys = new ArrayList<>(keys);
        this.date = date;
        this.duration = duration;
    }


    @Override
    public Type getType()
    {
        return this.type;
    }


    @Override
    public long getDuration()
    {
        return this.duration;
    }


    @Override
    public Date getDate()
    {
        return this.date;
    }


    @Override
    public CharSequence getInformation()
    {
        return this.type + " index [" + this.index + "] with values " + this.keys + ".";
    }
}
