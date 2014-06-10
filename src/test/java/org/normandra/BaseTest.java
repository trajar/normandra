package org.normandra;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.normandra.cassandra.CassandraTestHelper;
import org.normandra.orientdb.OrientTestHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * a base test case bootstrap
 * <p/>
 * User: bowen
 * Date: 6/8/14
 */
abstract public class BaseTest
{
    public final List<TestHelper> helpers;


    public BaseTest()
    {
        final List<TestHelper> list = new ArrayList<>(2);
        list.add(new CassandraTestHelper());
        list.add(new OrientTestHelper());
        this.helpers = Collections.unmodifiableList(list);
    }


    @BeforeClass
    public static void setup() throws Exception
    {
        CassandraTestHelper.setup();
        OrientTestHelper.setup();
    }


    @Before
    public void create() throws Exception
    {
        for (final TestHelper helper : helpers)
        {
            helper.create();
        }
    }


    @After
    public void destroy() throws Exception
    {
        for (final TestHelper helper : helpers)
        {
            helper.destroy();
        }
    }
}
