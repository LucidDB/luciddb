/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2002-2007 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package org.eigenbase.javac;

import java.io.*;

import java.nio.*;

import java.security.*;

import java.util.*;

import org.codehaus.janino.*;
import org.codehaus.janino.util.*;
import org.codehaus.janino.util.enumerator.*;
import org.codehaus.janino.util.resource.*;

import org.eigenbase.util.*;


/**
 * <code>JaninoCompiler</code> implements the {@link JavaCompiler} interface by
 * calling <a href="http://www.janino.net">Janino</a>.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JaninoCompiler
    implements JavaCompiler
{
    //~ Instance fields --------------------------------------------------------

    private JaninoCompilerArgs args = new JaninoCompilerArgs();

    // REVIEW jvs 28-June-2004:  pool this instance?  Is it thread-safe?
    private AccountingClassLoader classLoader;

    //~ Constructors -----------------------------------------------------------

    public JaninoCompiler()
    {
        args = new JaninoCompilerArgs();
    }

    //~ Methods ----------------------------------------------------------------

    // implement JavaCompiler
    public void compile()
    {
        // FIXME jvs 19-Feb-2007:  Should not need this synchronization,
        // but without it we get compilation problems inside of Janino
        // when invoked from the codeCache.mtsql concurrency test on
        // slow machines.  Get a fix for Janino and then remove this.
        synchronized (JaninoCompiler.class) {
            compileImpl();
        }
    }

    private void compileImpl()
    {
        // REVIEW: SWZ: 3/12/2006: When this method is invoked multiple times,
        // it creates a series of AccountingClassLoader objects, each with
        // the previous as its parent ClassLoader.  If we refactored this
        // class and its callers to specify all code to compile in one
        // go, we could probably just use a single AccountingClassLoader.

        // TODO jvs 10-Nov-2004: provide a means to request
        // DebuggingInformation.ALL

        assert (args.destdir != null);
        assert (args.fullClassName != null);
        assert (args.source != null);

        ClassLoader parentClassLoader = args.getClassLoader();
        if (classLoader != null) {
            parentClassLoader = classLoader;
        }

        Map<String, byte[]> sourceMap = new HashMap<String, byte[]>();
        sourceMap.put(
            ClassFile.getSourceResourceName(args.fullClassName),
            args.source.getBytes());
        MapResourceFinder sourceFinder = new MapResourceFinder(sourceMap);

        classLoader =
            new AccountingClassLoader(
                parentClassLoader,
                sourceFinder,
                null,
                DebuggingInformation.NONE);
        try {
            classLoader.loadClass(args.fullClassName);
        } catch (ClassNotFoundException ex) {
            throw Util.newInternal(ex, "while compiling " + args.fullClassName);
        }
    }

    // implement JavaCompiler
    public JavaCompilerArgs getArgs()
    {
        return args;
    }

    // implement JavaCompiler
    public ClassLoader getClassLoader()
    {
        return classLoader;
    }

    // implement JavaCompiler
    public int getTotalByteCodeSize()
    {
        return classLoader.getTotalByteCodeSize();
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class JaninoCompilerArgs
        extends JavaCompilerArgs
    {
        String destdir;
        String fullClassName;
        String source;

        public JaninoCompilerArgs()
        {
        }

        public boolean supportsSetSource()
        {
            return true;
        }

        public void setDestdir(String destdir)
        {
            super.setDestdir(destdir);
            this.destdir = destdir;
        }

        public void setSource(String source, String fileName)
        {
            this.source = source;
            addFile(fileName);
        }

        public void setFullClassName(String fullClassName)
        {
            this.fullClassName = fullClassName;
        }
    }

    /**
     * Refinement of JavaSourceClassLoader which keeps track of the total
     * bytecode length of the classes it has compiled.
     */
    private static class AccountingClassLoader
        extends JavaSourceClassLoader
    {
        private int nBytes;

        public AccountingClassLoader(
            ClassLoader parentClassLoader,
            ResourceFinder sourceFinder,
            String optionalCharacterEncoding,
            EnumeratorSet debuggingInformation)
        {
            super(
                parentClassLoader,
                sourceFinder,
                optionalCharacterEncoding,
                debuggingInformation);
        }

        int getTotalByteCodeSize()
        {
            return nBytes;
        }

        // override JavaSourceClassLoader
        protected Map generateBytecodes(String name)
            throws ClassNotFoundException
        {
            Map map = super.generateBytecodes(name);
            if (map == null) {
                return map;
            }

            // NOTE jvs 18-Oct-2006:  Janino has actually compiled everything
            // to bytecode even before all of the classes have actually
            // been loaded.  So we intercept their sizes here just
            // after they've been compiled.
            for (Object obj : map.values()) {
                byte [] bytes = (byte []) obj;
                nBytes += bytes.length;
            }
            return map;
        }
    }
}

// End JaninoCompiler.java
