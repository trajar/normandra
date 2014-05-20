package org.normandra.orientdb;

import org.apache.commons.lang.NullArgumentException;
import org.normandra.log.DatabaseActivity;

import java.util.Date;

/**
 * a orientdb database listActivity
 * <p/>
 * User: bowen
 * Date: 4/4/14
 */
public class OrientIndexActivity implements DatabaseActivity
{
    private final Type type;

    private final String index;

    private final Object key;

    private final Date date;

    private final long duration;


    public OrientIndexActivity(Type type, String index, Object key, Date date, long duration)
    {
        if (null == type)
        {
            throw new NullArgumentException("type");
        }
        if (null == index)
        {
            throw new NullArgumentException("index");
        }
        if (null == key)
        {
            throw new NullArgumentException("key");
        }
        if (null == date)
        {
            throw new NullArgumentException("date");
        }
        this.type = type;
        this.index = index;
        this.key = key;
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
        return "query index [" + this.index + "] with value {" + this.key + "}";
    }
}
