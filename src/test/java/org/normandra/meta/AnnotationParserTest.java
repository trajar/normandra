package org.normandra.meta;

import junit.framework.Assert;
import org.junit.Test;
import org.normandra.entities.CatEntity;
import org.normandra.entities.ClassEntity;
import org.normandra.entities.SimpleEntity;
import org.normandra.entities.StudentEntity;

import java.util.ArrayList;
import java.util.List;

/**
 * annotation parser unit tests
 * <p/>
 * User: bowen
 * Date: 9/1/13
 */
public class AnnotationParserTest
{
    @Test
    public void testSimple()
    {
        final AnnotationParser parser = new AnnotationParser(SimpleEntity.class);
        Assert.assertTrue(parser.isEntity(SimpleEntity.class));
        Assert.assertEquals("SimpleEntity", parser.getEntity(SimpleEntity.class));
        Assert.assertEquals("simple_entity", parser.getTable(SimpleEntity.class));
        final List<ColumnMeta> columns = parser.getColumns(SimpleEntity.class);
        Assert.assertFalse(columns.isEmpty());
        Assert.assertEquals(3, columns.size());
    }


    @Test
    public void testInheritance()
    {
        AnnotationParser parser = new AnnotationParser(CatEntity.class);
        Assert.assertTrue(parser.isEntity(CatEntity.class));
        Assert.assertEquals("CatEntity", parser.getEntity(CatEntity.class));
        Assert.assertEquals("animal", parser.getTable(CatEntity.class));
        EntityMeta meta = parser.read().iterator().next();
        Assert.assertTrue(meta.hasColumn("type"));
    }


    @Test
    public void testJoinColumn()
    {
        AnnotationParser parser = new AnnotationParser(ClassEntity.class, StudentEntity.class);
        Assert.assertTrue(parser.isEntity(ClassEntity.class));
        Assert.assertTrue(parser.isEntity(StudentEntity.class));
        List<EntityMeta> entities = new ArrayList<>(parser.read());
        Assert.assertEquals(2, entities.size());
        final EntityMeta student = entities.get(1);
        Assert.assertEquals(StudentEntity.class, student.getType());
        Assert.assertTrue(student.hasColumn("class_id"));
        Assert.assertEquals(Long.class, student.getColumn("class_id").getType());
        Assert.assertTrue(student.getColumn("class_id") instanceof JoinColumnMeta);
        Assert.assertEquals(entities.get(0), ((JoinColumnMeta)student.getColumn("class_id")).getEntity());
    }
}
