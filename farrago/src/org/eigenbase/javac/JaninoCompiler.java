/*
// $Id$
// Saffron preprocessor and data engine
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2002-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package org.eigenbase.javac;

import net.janino.*;
import org.eigenbase.util.*;

import java.io.*;

/**
 * <code>JaninoCompiler</code> implements the {@link JavaCompiler}
 * interface by calling <a href="http://www.janino.net">Janino</a>.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JaninoCompiler implements JavaCompiler
{
    private JaninoCompilerArgs args = new JaninoCompilerArgs();

    // REVIEW jvs 28-June-2004:  pool this instance?  Is it thread-safe?
    private JavaSourceClassLoader classLoader;

    public JaninoCompiler()
    {
        args = new JaninoCompilerArgs();
    }
    
    // implement JavaCompiler
    public void compile()
    {
        // REVIEW jvs 29-Sept-2004: we used to delegate to
        // ClassLoader.getSystemClassLoader(), but for some reason that didn't
        // work when run from ant's junit task without forking.  Should
        // probably take it as a parameter, but how should we decide what to
        // use?

        // TODO jvs 10-Nov-2004: provide a means to request
        // DebuggingInformation.ALL
        
        assert(args.destdir != null);
        assert(args.fullClassName != null);
        // TODO jvs 28-June-2004: with some glue code, we could probably get
        // Janino to compile directly from the generated string source instead
        // of from a file.  (It's possible to do that with the SimpleCompiler
        // class, but then we don't avoid the bytecode storage.)
        classLoader = new JavaSourceClassLoader(
            args.getClassLoader(), 
            new File[] { new File(args.destdir) },
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

    private static class JaninoCompilerArgs extends JavaCompilerArgs
    {
        String destdir;
        String fullClassName;
        
        public JaninoCompilerArgs()
        {
        }

        public void setDestdir(String destdir)
        {
            super.setDestdir(destdir);
            this.destdir = destdir;
        }

        // NOTE jvs 28-June-2004:  these go along with TODO above
        /*
        String source;
        public void setSource(String source, String fileName)
        {
            this.source = source;
            addFile(fileName);
        }
        */
        
        public void setFullClassName(String fullClassName)
        {
            this.fullClassName = fullClassName;
        }
    }
}

// End JaninoCompiler.java
