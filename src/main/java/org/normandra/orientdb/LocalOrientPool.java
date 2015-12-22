package org.normandra.orientdb;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

/**
 * a local on-demand orientdb connection pool
 */
public class LocalOrientPool implements OrientPool
{
    private final String url;

    private final String user;

    private final String password;

    public LocalOrientPool(final String url)
    {
        this(url, null, null);
    }

    public LocalOrientPool(final String url, final String user, final String pwd)
    {
        this.url = url;
        this.user = user;
        this.password = pwd;
    }

    @Override
    public ODatabaseDocumentTx acquire()
    {
        final ODatabaseDocumentTx db = new ODatabaseDocumentTx(this.url, false);
        db.activateOnCurrentThread();
        if (this.user != null || this.password != null)
        {
            db.open(this.user, this.password);
        }
        return db;
    }

    @Override
    public void close()
    {
        Orient.instance().closeAllStorages();
    }
}
