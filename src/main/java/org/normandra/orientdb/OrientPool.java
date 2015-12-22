package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

/**
 * orient connection pool api
 */
public interface OrientPool
{
    ODatabaseDocumentTx acquire();
    void close();
}
