package org.normandra;

import junit.framework.Assert;
import org.junit.Test;
import org.normandra.entities.CatEntity;
import org.normandra.entities.ClassEntity;
import org.normandra.entities.CompositeIndexEntity;
import org.normandra.entities.DogEntity;
import org.normandra.entities.StudentDirectoryEntity;
import org.normandra.entities.StudentEntity;
import org.normandra.entities.StudentIndexEntity;
import org.normandra.meta.EntityMeta;

import java.util.Map;

/**
 * unit test to test persistence
 * <p/>
 * User: bowen
 * Date: 1/20/14
 */
public class SaveTest extends BaseTest
{
    @Test
    public void testInherited() throws Exception
    {
        for (final TestHelper helper : helpers)
        {
            final Database database = helper.getDatabase();
            final DatabaseSession session = helper.getSession();
            final Map<Class, EntityMeta> entityMap = TestUtils.refresh(database, DogEntity.class, CatEntity.class);
            final DogEntity dog = new DogEntity("fido", 12);
            session.save(entityMap.get(DogEntity.class), dog);
            Assert.assertEquals(Long.valueOf(1), dog.getId());
            final CatEntity cat = new CatEntity("hank", true);
            session.save(entityMap.get(CatEntity.class), cat);
            Assert.assertEquals(Long.valueOf(2), cat.getId());
        }
    }


    @Test
    public void testJoinColumn() throws Exception
    {
        for (final TestHelper helper : helpers)
        {
            final Database database = helper.getDatabase();
            final DatabaseSession session = helper.getSession();
            final Map<Class, EntityMeta> entityMap = TestUtils.refresh(database, StudentEntity.class, ClassEntity.class);
            final ClassEntity classroom = new ClassEntity("geopolitics", 234);
            session.save(entityMap.get(ClassEntity.class), classroom);
            Assert.assertTrue(classroom == session.get(entityMap.get(ClassEntity.class), 1L));
            session.clear();
            Assert.assertEquals(classroom, session.get(entityMap.get(ClassEntity.class), 1L));

            StudentEntity student = new StudentEntity("fred");
            session.save(entityMap.get(StudentEntity.class), student);
            Assert.assertNull(student.getClassroom());
            session.clear();

            student = (StudentEntity) session.get(entityMap.get(StudentEntity.class), student.getId());
            Assert.assertNotNull(student);
            Assert.assertNull(student.getClassroom());

            student.setClassroom(classroom);
            session.save(entityMap.get(StudentEntity.class), student);
            session.clear();

            student = (StudentEntity) session.get(entityMap.get(StudentEntity.class), student.getId());
            Assert.assertNotNull(student);
            Assert.assertNotNull(student.getClassroom());
            Assert.assertEquals(classroom.getId(), student.getClassroom().getId());
            Assert.assertEquals(classroom.getName(), student.getClassroom().getName());
            Assert.assertEquals(classroom.getRoom(), student.getClassroom().getRoom());
            session.clear();

            session.save(entityMap.get(StudentEntity.class), new StudentEntity("bob", classroom));
            session.save(entityMap.get(StudentEntity.class), new StudentEntity("jane", classroom));
            session.clear();

            final ClassEntity associated = (ClassEntity) session.get(entityMap.get(ClassEntity.class), classroom.getId());
            Assert.assertNotNull(associated);
            Assert.assertNotNull(associated.getStudents());
            Assert.assertEquals(3, associated.getStudents().size());
            Assert.assertTrue(associated.getStudents().contains(student));
        }
    }


    @Test
    public void testJoinTable() throws Exception
    {
        for (final TestHelper helper : helpers)
        {
            final Database database = helper.getDatabase();
            final DatabaseSession session = helper.getSession();
            final Map<Class, EntityMeta> entityMap = TestUtils.refresh(database, StudentEntity.class, ClassEntity.class, StudentDirectoryEntity.class);

            final StudentDirectoryEntity calcStudents = new StudentDirectoryEntity("calc students");
            session.save(entityMap.get(StudentDirectoryEntity.class), calcStudents);
            Assert.assertTrue(calcStudents == session.get(entityMap.get(StudentDirectoryEntity.class), 1L));
            session.clear();
            Assert.assertEquals(calcStudents, session.get(entityMap.get(StudentDirectoryEntity.class), 1L));

            // create collection
            final StudentEntity bob = new StudentEntity("bob");
            final StudentEntity jane = new StudentEntity("jane");
            final StudentEntity bobo = new StudentEntity("bobo");
            calcStudents.addStudent(bob);
            calcStudents.addStudent(jane);
            calcStudents.addStudent(bobo);
            session.beginWork();
            for (final StudentEntity student : calcStudents.getStudents())
            {
                session.save(entityMap.get(StudentEntity.class), student);
            }
            session.save(entityMap.get(StudentDirectoryEntity.class), calcStudents);
            session.commitWork();

            // we should be able to retrieve join collection
            session.clear();
            final StudentDirectoryEntity existing = (StudentDirectoryEntity) session.get(entityMap.get(StudentDirectoryEntity.class), calcStudents.getId());
            Assert.assertNotNull(existing);
            Assert.assertEquals(3, existing.getStudents().size());
            Assert.assertTrue(existing.getStudents().contains(bob));
            Assert.assertTrue(existing.getStudents().contains(jane));
            Assert.assertTrue(existing.getStudents().contains(bobo));

            // we should be able to remove an item
            session.clear();
            calcStudents.removeStudent(bobo);
            session.save(entityMap.get(StudentDirectoryEntity.class), calcStudents);
            session.clear();
            final StudentDirectoryEntity existing2 = (StudentDirectoryEntity) session.get(entityMap.get(StudentDirectoryEntity.class), calcStudents.getId());
            Assert.assertNotNull(existing2);
            Assert.assertEquals(2, existing2.getStudents().size());
            Assert.assertTrue(existing2.getStudents().contains(bob));
            Assert.assertTrue(existing2.getStudents().contains(jane));
            Assert.assertFalse(existing2.getStudents().contains(bobo));
        }
    }


    @Test
    public void testComposite() throws Exception
    {
        for (final TestHelper helper : helpers)
        {
            final Database database = helper.getDatabase();
            final DatabaseSession session = helper.getSession();
            final Map<Class, EntityMeta> entityMap = TestUtils.refresh(database, StudentIndexEntity.class, CompositeIndexEntity.class);
            final EntityMeta studentMeta = entityMap.get(StudentIndexEntity.class);
            final EntityMeta compositeMeta = entityMap.get(CompositeIndexEntity.class);

            final StudentIndexEntity student = new StudentIndexEntity("fred", 101);
            session.save(studentMeta, student);
            Assert.assertNull(session.get(studentMeta, "fred"));
            Assert.assertNull(session.get(studentMeta, "101"));

            final CompositeIndexEntity composite = new CompositeIndexEntity("foo");
            session.save(compositeMeta, composite);
            Assert.assertNotNull(composite.getId());
            Assert.assertNotNull(composite.getName());
            session.clear();
            final CompositeIndexEntity existing = (CompositeIndexEntity) session.get(compositeMeta, new CompositeIndexEntity.Key(composite.getId(), composite.getName()));
            Assert.assertNotNull(existing);
            Assert.assertEquals(composite.getId(), existing.getId());
            Assert.assertEquals(composite.getName(), existing.getName());
            Assert.assertNull(session.get(compositeMeta, composite.getId()));
            Assert.assertNull(session.get(compositeMeta, composite.getName()));
        }
    }
}
