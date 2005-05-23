/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * A <code>JavaCompilerArgs</code> holds the arguments for a {@link
 * JavaCompiler}.
 *
 * <p> Specific implementations of {@link JavaCompiler} may override
 * <code>set<i>Argument</i></code> methods to store arguments in a different
 * fashion, or may throw {@link UnsupportedOperationException} to indicate that
 * the compiler does not support that argument.
 *
 * @author jhyde
 * @since Jun 2, 2002
 * @version $Id$
 */
public class JavaCompilerArgs
{
    ArrayList argsList = new ArrayList();
    ArrayList fileNameList = new ArrayList();

    ClassLoader classLoader;

    public JavaCompilerArgs()
    {
        classLoader = getClass().getClassLoader();
    }
    
    public void clear()
    {
        fileNameList.clear();
    }
    
    /**
     * Sets the arguments by parsing a standard java argument string.
     *
     * <p>A typical such string is
     * <code>"-classpath <i>classpath</i> -d <i>dir</i> -verbose
     * [<i>file</i>...]"</code>
     */
    public void setString(String args)
    {
        ArrayList list = new ArrayList();
        StringTokenizer tok = new StringTokenizer(args);
        while (tok.hasMoreTokens()) {
            list.add(tok.nextToken());
        }
        setStringArray((String[]) list.toArray(new String[list.size()]));
    }
    
    /**
     * Sets the arguments by parsing a standard java argument string.
     *
     * A typical such string is
     * <code>"-classpath <i>classpath</i> -d <i>dir</i> -verbose
     * [<i>file</i>...]"</code>
     */
    public void setStringArray(String[] args)
    {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-classpath")) {
                if (++i < args.length) {
                    setClasspath(args[i]);
                }
            } else if (arg.equals("-d")) {
                if (++i < args.length) {
                    setDestdir(args[i]);
                }
            } else if (arg.equals("-verbose")) {
                setVerbose(true);
            } else {
                argsList.add(args[i]);
            }
        }
    }
    
    public String[] getStringArray()
    {
        for (Iterator fileNames = fileNameList.iterator();
                fileNames.hasNext();) {
            String fileName = (String) fileNames.next();
            argsList.add(fileName);
        }
        return (String[]) argsList.toArray(new String[0]);
    }
    
    public void addFile(String fileName)
    {
        fileNameList.add(fileName);
    }
    
    public String[] getFileNames()
    {
        return (String[]) fileNameList.toArray(new String[0]);
    }
    
    public void setVerbose(boolean verbose)
    {
        if (verbose) {
            argsList.add("-verbose");
        }
    }
    
    public void setDestdir(String destdir)
    {
        argsList.add("-d");
        argsList.add(destdir);
    }
    
    public void setClasspath(String classpath)
    {
        argsList.add("-classpath");
        argsList.add(classpath);
    }
    
    public void setDebugInfo(int i)
    {
        if (i > 0) {
            argsList.add("-g=" + i);
        }
    }
    
    /**
     * Sets the source code (that is, the full java program, generally starting
     * with something like "package com.foo.bar;") and the file name.
     *
     * <p>This method is optional. It only works if the compiler supports
     * in-memory compilation. If this compiler does not return in-memory
     * compilation (which the base class does not), {@link #supportsSetSource}
     * returns false, and this method throws
     * {@link UnsupportedOperationException}.
     */
    public void setSource(String source, String fileName)
    {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Returns whether {@link #setSource} will work.
     */
    public boolean supportsSetSource()
    {
        return false;
    }
    
    public void setFullClassName(String fullClassName)
    {
        // NOTE jvs 28-June-2004: I added this in order to support Janino's
        // JavaSourceClassLoader, which needs it.  Non-Farrago users
        // don't need to call this method.
    }

    public void setClassLoader(ClassLoader classLoader)
    {
        this.classLoader = classLoader;
    }

    public ClassLoader getClassLoader()
    {
        return classLoader;
    }
}

// End JavaCompilerArgs.java
