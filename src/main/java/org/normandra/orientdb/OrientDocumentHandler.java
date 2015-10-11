package org.normandra.orientdb;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * a simple class for handling document instances
 * <p/>
 * 
 * Date: 6/6/14
 */
public interface OrientDocumentHandler
{
    Object convert(ODocument document);
}
