package org.normandra.orientdb;

import com.orientechnologies.orient.core.id.ORID;
import org.normandra.log.DatabaseActivity;

import java.util.Date;

/**
 * update activity record
 * <p/>
 * User: bowen
 * Date: 5/20/14
 */
public class OrientUpdateActivity implements DatabaseActivity
{
    private final Type type;

    private final ORID rid;

    private final Date date;

    private final long duration;


    public OrientUpdateActivity(Type type, ORID rid, Date date, long duration)
    {
        this.type = type;
        this.rid = rid;
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
        return this.type + " document [" + this.rid + "]";
    }
}
