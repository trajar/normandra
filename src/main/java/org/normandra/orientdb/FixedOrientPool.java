package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

/**
 * fixed size orientdb connection pool
 */
public class FixedOrientPool implements OrientPool
{
    private static final int poolSize = 4;

    private final OPartitionedDatabasePool pool;

    public FixedOrientPool(final String url, final String user, final String pwd)
    {
        this.pool = new OPartitionedDatabasePool(url, user, pwd, poolSize);
    }

    @Override
    public ODatabaseDocumentTx acquire()
    {
        return this.pool.acquire();
    }

    @Override
    public void close()
    {
        this.pool.close();
    }
}
