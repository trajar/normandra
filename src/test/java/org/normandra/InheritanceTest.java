package org.normandra;

import junit.framework.Assert;
import org.junit.Test;
import org.normandra.entities.AnimalEntity;
import org.normandra.entities.CatEntity;
import org.normandra.entities.DogEntity;
import org.normandra.entities.ZooEntity;

import java.util.Arrays;

/**
 * unit tests for inheritance and abstract entity classes
 * <p/>
 * User: bowen
 * Date: 2/1/14
 */
public class InheritanceTest extends BaseTest
{
    @Test
    public void testSave() throws Exception
    {
        for (final TestHelper helper : helpers)
        {
            final EntityManager manager = helper.getManager();
            Assert.assertFalse(manager.exists(AnimalEntity.class, 1));
            Assert.assertFalse(manager.exists(AnimalEntity.class, 1));

            final DogEntity dog = new DogEntity("sophi", 12);
            manager.save(dog);

            Assert.assertTrue(manager.exists(DogEntity.class, 1));
            Assert.assertTrue(manager.exists(AnimalEntity.class, 1));
            Assert.assertEquals(dog, manager.get(DogEntity.class, 1));
            Assert.assertEquals(dog, manager.get(AnimalEntity.class, 1));
        }
    }


    @Test
    public void testHierarchy() throws Exception
    {
        for (final TestHelper helper : helpers)
        {
            final EntityManager manager = helper.getManager();
            final DogEntity dog = new DogEntity("sophi", 12);
            final CatEntity cat = new CatEntity("kitty", true);
            manager.save(dog);
            manager.save(cat);

            final ZooEntity zoo = new ZooEntity(Arrays.asList(dog, cat));
            manager.save(zoo);
            manager.clear();

            final ZooEntity existing = manager.get(ZooEntity.class, zoo.getId());
            Assert.assertNotNull(existing);
            Assert.assertEquals(2, existing.getAnimals().size());
            Assert.assertTrue(existing.getAnimals().contains(dog));
            Assert.assertTrue(existing.getAnimals().contains(cat));
        }
    }
}
