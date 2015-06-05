package org.normandra.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CompositeClassLoader extends ClassLoader
{
    private final List<ClassLoader> loaders;


    public CompositeClassLoader(final ClassLoader parent, final Collection<ClassLoader> delegates)
    {
        super(parent);
        this.loaders = new ArrayList<>(delegates);
    }


    @Override
    public Class loadClass(final String name) throws ClassNotFoundException
    {
        for (final ClassLoader loader : this.loaders)
        {
            try
            {
                return loader.loadClass(name);
            }
            catch (ClassNotFoundException notFound)
            {

            }
        }

        return super.loadClass(name);
    }
}
