package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import org.normandra.EntitySession;
import org.normandra.NormandraException;
import org.normandra.association.BasicElementFactory;
import org.normandra.meta.EntityContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * User: bowen
 * Date: 9/23/14
 */
public class OrientElementFactory<T> extends BasicElementFactory<T>
{
    public OrientElementFactory(final EntityContext entity)
    {
        super(entity);
    }


    @Override
    public ORID pack(final EntitySession session, final T value) throws NormandraException
    {
        final Object key = this.getEntity().getId().fromEntity(value);
        if (null == key)
        {
            return null;
        }
        final OrientDatabaseSession orientdb = (OrientDatabaseSession) session;
        final OIdentifiable rid = orientdb.findIdByKey(this.getEntity(), key);
        return rid != null ? rid.getIdentity() : null;
    }


    @Override
    public List<?> pack(final EntitySession session, final T... values) throws NormandraException
    {
        if (null == values || values.length <= 0)
        {
            return Collections.emptyList();
        }
        final OrientDatabaseSession orientdb = (OrientDatabaseSession) session;
        final List<ORID> list = new ArrayList<>(values.length);
        for (final Object value : values)
        {
            final Object key = this.getEntity().getId().fromEntity(value);
            final OIdentifiable item = orientdb.findIdByKey(this.getEntity(), key);
            final ORID rid = item != null ? item.getIdentity() : null;
            if (rid != null)
            {
                list.add(rid);
            }
        }
        return Collections.unmodifiableList(list);
    }
}
