package org.normandra.cassandra;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.normandra.entities.CatEntity;
import org.normandra.entities.ClassEntity;
import org.normandra.entities.CompositeIndexEntity;
import org.normandra.entities.DogEntity;
import org.normandra.entities.SimpleEntity;
import org.normandra.entities.StudentEntity;
import org.normandra.entities.StudentIndexEntity;
import org.normandra.meta.AnnotationParser;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityMeta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * unit test to test persistence
 * <p>
 * User: bowen
 * Date: 1/20/14
 */
public class CassandraSaveTest extends BaseCassandraTest
{
    private CassandraDatabase database;

    private CassandraDatabaseSession session;


    @Before
    public void create() throws Exception
    {
        this.database = new CassandraDatabaseFactory(keyspace, "localhost", port, construction).create();
        this.session = this.database.createSession();
    }


    @After
    public void destroy() throws IOException
    {
        if (this.session != null)
        {
            this.session.close();
            this.session = null;
        }
        if (this.database != null)
        {
            this.database.close();
            this.database = null;
        }
        CassandraUtil.reset();
    }


    private Map<Class, EntityMeta> setupEntities(final Class... entities) throws Exception
    {
        final Map<Class, EntityMeta> map = new HashMap<>();
        final AnnotationParser parser = new AnnotationParser(Arrays.asList(entities));
        final List<EntityMeta> list = new ArrayList<>();
        list.addAll(parser.read());
        for (final EntityMeta meta : list)
        {
            map.put(meta.getType(), meta);
        }
        final DatabaseMeta meta = new DatabaseMeta(list);
        this.database.refresh(meta);
        return Collections.unmodifiableMap(map);
    }


    @Test
    public void testSimple() throws Exception
    {
        final Map<Class, EntityMeta> entityMap = this.setupEntities(SimpleEntity.class);
        final EntityMeta meta = entityMap.values().iterator().next();
        final SimpleEntity entity = new SimpleEntity("test", Arrays.asList("foo", "bar"));
        this.session.save(meta, entity);
        Assert.assertEquals(1, entity.getId());
        Assert.assertEquals(1, this.session.getActivity().size());

        Assert.assertFalse(this.session.exists(meta, 0L));
        Assert.assertTrue(this.session.exists(meta, 1L));
        Assert.assertTrue(this.session.exists(meta, 1));
        Assert.assertEquals(4, this.session.getActivity().size());

        final SimpleEntity notfound = (SimpleEntity) this.session.get(meta, 0);
        Assert.assertNull(notfound);
        Assert.assertEquals(5, this.session.getActivity().size());
        final SimpleEntity existing = (SimpleEntity) this.session.get(meta, 1);
        Assert.assertNotNull(existing);
        Assert.assertEquals(1, existing.getId());
        Assert.assertEquals(6, this.session.getActivity().size());

        this.session.delete(meta, existing);
        Assert.assertEquals(7, this.session.getActivity().size());
        Assert.assertFalse(this.session.exists(meta, 1));
        Assert.assertEquals(8, this.session.getActivity().size());
        Assert.assertNull(this.session.get(meta, 1));
        Assert.assertEquals(9, this.session.getActivity().size());
    }


    @Test
    public void testInherited() throws Exception
    {
        final Map<Class, EntityMeta> entityMap = this.setupEntities(DogEntity.class, CatEntity.class);
        final DogEntity dog = new DogEntity("fido", 12);
        this.session.save(entityMap.get(DogEntity.class), dog);
        Assert.assertEquals(Long.valueOf(1), dog.getId());
        final CatEntity cat = new CatEntity("hank", true);
        this.session.save(entityMap.get(CatEntity.class), cat);
        Assert.assertEquals(Long.valueOf(2), cat.getId());
    }


    @Test
    public void testJoinColumn() throws Exception
    {
        final Map<Class, EntityMeta> entityMap = this.setupEntities(StudentEntity.class, ClassEntity.class);
        final ClassEntity classroom = new ClassEntity("geopolitics", 234);
        this.session.save(entityMap.get(ClassEntity.class), classroom);
        Assert.assertTrue(classroom == this.session.get(entityMap.get(ClassEntity.class), 1L));
        this.session.clear();
        Assert.assertEquals(classroom, this.session.get(entityMap.get(ClassEntity.class), 1L));

        StudentEntity student = new StudentEntity("fred");
        this.session.save(entityMap.get(StudentEntity.class), student);
        Assert.assertNull(student.getClassroom());
        this.session.clear();

        student = (StudentEntity) this.session.get(entityMap.get(StudentEntity.class), student.getId());
        Assert.assertNotNull(student);
        Assert.assertNull(student.getClassroom());

        student.setClassroom(classroom);
        this.session.save(entityMap.get(StudentEntity.class), student);
        this.session.clear();

        student = (StudentEntity) this.session.get(entityMap.get(StudentEntity.class), student.getId());
        Assert.assertNotNull(student);
        Assert.assertNotNull(student.getClassroom());
        Assert.assertEquals(classroom.getId(), student.getClassroom().getId());
        Assert.assertEquals(classroom.getName(), student.getClassroom().getName());
        Assert.assertEquals(classroom.getRoom(), student.getClassroom().getRoom());
    }


    @Test
    public void testJoinTable() throws Exception
    {
        final Map<Class, EntityMeta> entityMap = this.setupEntities(StudentEntity.class, ClassEntity.class);

        final ClassEntity classroom = new ClassEntity("calculus", 101);
        this.session.save(entityMap.get(ClassEntity.class), classroom);
        Assert.assertTrue(classroom == this.session.get(entityMap.get(ClassEntity.class), 1L));
        this.session.clear();
        Assert.assertEquals(classroom, this.session.get(entityMap.get(ClassEntity.class), 1L));
        final StudentEntity bob = new StudentEntity("bob");
        final StudentEntity jane = new StudentEntity("jane");
        classroom.addStudent(bob);
        classroom.addStudent(jane);
        this.session.beginWork();
        for (final StudentEntity student : classroom.getStudents())
        {
            this.session.save(entityMap.get(StudentEntity.class), student);
        }
        this.session.save(entityMap.get(ClassEntity.class), classroom);
        this.session.commitWork();

        this.session.clear();
        final ClassEntity existing = (ClassEntity) this.session.get(entityMap.get(ClassEntity.class), classroom.getId());
        Assert.assertNotNull(existing);
        Assert.assertEquals(2, existing.getStudents().size());
        Assert.assertTrue(existing.getStudents().contains(bob));
        Assert.assertTrue(existing.getStudents().contains(jane));
    }


    @Test
    public void testComposite() throws Exception
    {
        final Map<Class, EntityMeta> entityMap = this.setupEntities(StudentIndexEntity.class, CompositeIndexEntity.class);
        final EntityMeta studentMeta = entityMap.get(StudentIndexEntity.class);
        final EntityMeta compositeMeta = entityMap.get(CompositeIndexEntity.class);

        final StudentIndexEntity student = new StudentIndexEntity("fred", 101);
        this.session.save(studentMeta, student);
        Assert.assertNull(session.get(studentMeta, "fred"));
        Assert.assertNull(session.get(studentMeta, "101"));

        final CompositeIndexEntity composite = new CompositeIndexEntity("foo");
        this.session.save(compositeMeta, composite);
        Assert.assertNotNull(composite.getId());
        Assert.assertNotNull(composite.getName());
        this.session.clear();
        final CompositeIndexEntity existing = (CompositeIndexEntity) this.session.get(compositeMeta, new CompositeIndexEntity.Key(composite.getId(), composite.getName()));
        Assert.assertNotNull(existing);
        Assert.assertEquals(composite.getId(), existing.getId());
        Assert.assertEquals(composite.getName(), existing.getName());
        Assert.assertNull(this.session.get(compositeMeta, composite.getId()));
        Assert.assertNull(this.session.get(compositeMeta, composite.getName()));
    }
}
