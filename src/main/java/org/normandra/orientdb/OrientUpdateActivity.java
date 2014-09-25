package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import org.normandra.log.DatabaseActivity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * update activity record
 * <p/>
 * User: bowen
 * Date: 5/20/14
 */
public class OrientUpdateActivity implements DatabaseActivity
{
    private final Type type;

    private final List<OIdentifiable> rids;

    private final Date date;

    private final long duration;


    public OrientUpdateActivity(Type type, Collection<? extends OIdentifiable> rids, Date date, long duration)
    {
        this.type = type;
        this.rids = new ArrayList<>(rids);
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
        return this.type + " documents " + this.rids + ".";
    }
}
