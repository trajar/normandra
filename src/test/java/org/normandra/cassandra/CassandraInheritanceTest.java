package org.normandra.cassandra;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.normandra.NormandraEntityManager;
import org.normandra.NormandraEntityManagerFactory;
import org.normandra.meta.AnimalEntity;
import org.normandra.meta.CatEntity;
import org.normandra.meta.DogEntity;

import java.io.IOException;

/**
 * unit tests for inheritance and abstract entity classes
 * <p/>
 * User: bowen
 * Date: 2/1/14
 */
public class CassandraInheritanceTest extends BaseCassandraTest
{
    private NormandraEntityManager manager;


    @Before
    public void create() throws Exception
    {
        final NormandraEntityManagerFactory factory = new NormandraEntityManagerFactory.Builder()
                .withClass(CatEntity.class)
                .withClass(DogEntity.class)
                .withParameter(CassandraDatabase.HOSTS, "localhost")
                .withParameter(CassandraDatabase.PORT, 9142)
                .withParameter(CassandraDatabase.KEYSPACE, keyspace)
                .withDatabaseConstruction(construction)
                .create();
        this.manager = factory.create();
    }


    @After
    public void destroy() throws IOException
    {
        if (this.manager != null)
        {
            this.manager.close();
            this.manager = null;
        }
        CassandraUtil.reset();
    }


    @Test
    public void testSave() throws Exception
    {
        final DogEntity dog = new DogEntity("sophi", 12);
        this.manager.save(dog);
        Assert.assertEquals(dog, this.manager.get(DogEntity.class, 1));
        Assert.assertEquals(dog, this.manager.get(AnimalEntity.class, 1));
//      Assert.assertNull(this.manager.get(CatEntity.class, 1));
    }
}
