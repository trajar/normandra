package org.normandra;

import org.normandra.meta.AnnotationParser;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityMeta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * test utilities
 * <p/>
 * User: bowen
 * Date: 6/8/14
 */
public class TestUtils
{
    public static Map<Class, EntityMeta> refresh(final Database database, final Class... entities) throws Exception
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
        database.refresh(meta);
        return Collections.unmodifiableMap(map);
    }


    private TestUtils()
    {
    }
}