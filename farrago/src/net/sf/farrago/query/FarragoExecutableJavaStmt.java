/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

package net.sf.farrago.query;

import net.sf.farrago.runtime.*;
import net.sf.farrago.type.*;

import net.sf.saffron.util.*;
import net.sf.saffron.core.*;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.lang.reflect.*;

/**
 * FarragoExecutableJavaStmt implements FarragoExecutableStmt via a compiled
 * Java class.
 *
 *<p>
 *
 * NOTE:  be sure to read superclass warnings before modifying this class.
 *
 *<p>
 *
 * TODO:  another implementation, FarragoExecutableFennelStmt, which operates
 * off of a pure Fennel plan; for use when there is no Java needed in the
 * implementation
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoExecutableJavaStmt extends FarragoExecutableStmtImpl
{
    private final File packageDir;
    
    private final Class rowClass;
    
    private final SaffronType rowType;

    private final Method method;

    FarragoExecutableJavaStmt(
        File packageDir,
        Class rowClass,
        SaffronType preparedRowType,
        SaffronType dynamicParamRowType,
        Method method,
        boolean isDml)
    {
        super(dynamicParamRowType,isDml);

        this.packageDir = packageDir;
        this.rowClass = rowClass;
        this.method = method;

        rowType = forgetTypeFactory(preparedRowType);
    }

    // implement FarragoExecutableStmt
    public SaffronType getRowType()
    {
        return rowType;
    }
    
    // implement FarragoExecutableStmt
    public ResultSet execute(
        FarragoRuntimeContext runtimeContext)
    {
        try {
            Iterator iter = (Iterator) method.invoke(
                null,
                new Object [] 
                {
                    runtimeContext
                });
            ResultSet resultSet = new FarragoIteratorResultSet(
                iter,
                rowClass,
                rowType,
                runtimeContext);
            runtimeContext = null;
            return resultSet;
        } catch (IllegalAccessException e) {
            throw Util.newInternal(e);
        } catch (InvocationTargetException e) {
            throw Util.newInternal(e);
        } finally {
            if (runtimeContext != null) {
                runtimeContext.closeAllocation();
            }
        }
    }
    
    // implement FarragoExecutableStmt
    public long getMemoryUsage()
    {

        // TODO: a better approximation.  This only sums the bytecode size of
        // the compiled classes.  Other allocations to estimate are loaded
        // class overhead (e.g. constants and reflection info), type
        // descriptor, JIT code size, and "this" object and fields such as
        // packageDir.
        
        long nBytes = 0;
        File [] files = packageDir.listFiles();
        for (int i = 0; i < files.length; ++i) {
            if (!files[i].getName().endsWith(".class")) {
                continue;
            }
            nBytes += files[i].length();
        }
        return nBytes;
    }
}

// End FarragoExecutableJavaStmt.java
