package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;
import org.normandra.DatabaseConstruction;

import java.io.File;

/**
 * common orientdb unit test bootstrap
 * <p/>
 * User: bowen
 * Date: 1/20/14
 */
public class BaseOrientTest
{
    public static final DatabaseConstruction construction = DatabaseConstruction.RECREATE;

    public final File dir = new File("target/embeddedOrientDB");

    public final String path = "plocal:" + dir.getPath();

    public final String user = "admin";

    public final String password = "admin";


    @Before
    public void createLocal() throws Exception
    {
        BasicConfigurator.configure();
        FileUtils.deleteDirectory(dir);
        final ODatabaseDocumentTx db = new ODatabaseDocumentTx(path).create();
        db.close();
    }


    @After
    public void detroyLocal() throws Exception
    {
        FileUtils.deleteDirectory(dir);
    }
}
