package org.normandra.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.BasicConfigurator;
import org.junit.BeforeClass;
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

    public static final File dir = new File("target/embeddedOrientDB");

    public static final String path = "plocal:" + dir.getPath();

    public static final String user = "admin";

    public static final String password = "admin";


    @BeforeClass
    public static void setupOrientDB() throws Exception
    {
        BasicConfigurator.configure();
        if(dir.exists())
        {
            FileUtils.deleteDirectory(dir);
        }
        new ODatabaseDocumentTx(path).create().close();
    }
}
