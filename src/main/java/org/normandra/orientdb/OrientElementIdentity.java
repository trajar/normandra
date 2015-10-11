package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import org.normandra.EntitySession;
import org.normandra.NormandraException;
import org.normandra.association.BasicElementIdentity;
import org.normandra.meta.EntityContext;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.SingleEntityContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 
 * Date: 9/23/14
 */
public class OrientElementIdentity<T> extends BasicElementIdentity<T>
{
    public OrientElementIdentity(final EntityMeta entity)
    {
        this(new SingleEntityContext(entity));
    }


    public OrientElementIdentity(final EntityContext entity)
    {
        super(entity);
    }


    @Override
    public ORID fromKey(final EntitySession session, final Object key) throws NormandraException
    {
        if (null == key)
        {
            return null;
        }
        final OrientDatabaseSession orientdb = (OrientDatabaseSession) session;
        final OIdentifiable rid = orientdb.findIdByKey(this.getEntity(), key);
        return rid != null ? rid.getIdentity() : null;
    }


    @Override
    public ORID fromEntity(final EntitySession session, final T value) throws NormandraException
    {
        if (null == value)
        {
            return null;
        }
        final Object key = this.getEntity().getId().fromEntity(value);
        return this.fromKey(session, key);
    }


    @Override
    public List<?> fromEntities(final EntitySession session, final T... values) throws NormandraException
    {
        if (null == values || values.length <= 0)
        {
            return Collections.emptyList();
        }
        final List<ORID> list = new ArrayList<>(values.length);
        for (final Object value : values)
        {
            final Object key = this.getEntity().getId().fromEntity(value);
            final ORID rid = this.fromKey(session, key);
            if (rid != null)
            {
                list.add(rid);
            }
        }
        return Collections.unmodifiableList(list);
    }
}
