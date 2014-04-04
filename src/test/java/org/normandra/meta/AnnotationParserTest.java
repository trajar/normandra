package org.normandra.meta;

import junit.framework.Assert;
import org.junit.Test;
import org.normandra.entities.CatEntity;
import org.normandra.entities.ClassEntity;
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
    public void testInheritance()
    {
        AnnotationParser parser = new AnnotationParser(CatEntity.class);
        Assert.assertTrue(parser.isEntity(CatEntity.class));
        Assert.assertEquals("CatEntity", parser.getEntity(CatEntity.class));
        EntityMeta meta = parser.read().iterator().next();
        Assert.assertEquals(1, meta.getTables().size());
        TableMeta table = meta.getTables().iterator().next();
        Assert.assertEquals("animal", table.getName());
        Assert.assertTrue(table.hasColumn("type"));
    }


    @Test
    public void testJoinColumn()
    {
        AnnotationParser parser = new AnnotationParser(ClassEntity.class, StudentEntity.class);
        Assert.assertTrue(parser.isEntity(ClassEntity.class));
        Assert.assertTrue(parser.isEntity(StudentEntity.class));
        List<EntityMeta> entities = new ArrayList<>(parser.read());
        Assert.assertEquals(2, entities.size());
        EntityMeta student = entities.get(1);
        TableMeta table = student.getTables().iterator().next();
        Assert.assertEquals(StudentEntity.class, student.getType());
        Assert.assertTrue(table.hasColumn("class_id"));
        Assert.assertEquals(Long.class, table.getColumn("class_id").getType());
        Assert.assertTrue(table.getColumn("class_id") instanceof JoinColumnMeta);
        Assert.assertEquals(entities.get(0), ((JoinColumnMeta) table.getColumn("class_id")).getEntity());
    }
}
