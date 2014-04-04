package org.normandra.cassandra;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.normandra.EntityManager;
import org.normandra.EntityManagerFactory;
import org.normandra.entities.AnimalEntity;
import org.normandra.entities.CatEntity;
import org.normandra.entities.DogEntity;
import org.normandra.entities.ZooEntity;

import java.io.IOException;
import java.util.Arrays;

/**
 * unit tests for inheritance and abstract entity classes
 * <p>
 * User: bowen
 * Date: 2/1/14
 */
public class CassandraInheritanceTest extends BaseCassandraTest
{
    private EntityManager manager;


    @Before
    public void create() throws Exception
    {
        final EntityManagerFactory factory = new EntityManagerFactory.Builder()
                .withClass(CatEntity.class)
                .withClass(DogEntity.class)
                .withClass(ZooEntity.class)
                .withParameter(CassandraDatabase.HOSTS, "localhost")
                .withParameter(CassandraDatabase.PORT, port)
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
        Assert.assertFalse(this.manager.exists(AnimalEntity.class, 1));
        Assert.assertFalse(this.manager.exists(AnimalEntity.class, 1));

        final DogEntity dog = new DogEntity("sophi", 12);
        this.manager.save(dog);

        Assert.assertTrue(this.manager.exists(DogEntity.class, 1));
        Assert.assertTrue(this.manager.exists(AnimalEntity.class, 1));
        Assert.assertEquals(dog, this.manager.get(DogEntity.class, 1));
        Assert.assertEquals(dog, this.manager.get(AnimalEntity.class, 1));
    }


    @Test
    public void testHierarchy() throws Exception
    {
        final DogEntity dog = new DogEntity("sophi", 12);
        final CatEntity cat = new CatEntity("kitty", true);
        this.manager.save(dog);
        this.manager.save(cat);
        final ZooEntity zoo = new ZooEntity(Arrays.asList(dog, cat));
        this.manager.save(zoo);
        this.manager.clear();
        final ZooEntity existing = this.manager.get(ZooEntity.class, zoo.getId());
        Assert.assertNotNull(existing);
        Assert.assertEquals(2, existing.getAnimals().size());
    }
}
