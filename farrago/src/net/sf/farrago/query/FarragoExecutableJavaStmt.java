/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
package net.sf.farrago.query;

import java.io.*;

import java.lang.reflect.*;

import java.sql.*;

import java.util.*;

import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.runtime.*;
import org.eigenbase.util.*;


/**
 * FarragoExecutableJavaStmt implements FarragoSessionExecutableStmt via a
 * compiled Java class.
 *
 * <p>NOTE: be sure to read superclass warnings before modifying this class.
 *
 * <p>TODO: another implementation, FarragoExecutableFennelStmt, which operates
 * off of a pure Fennel plan; for use when there is no Java needed in the
 * implementation
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoExecutableJavaStmt
    extends FarragoExecutableStmtImpl
{

    //~ Instance fields --------------------------------------------------------

    private final File packageDir;

    // TODO jvs 4-June-2004:  don't pin a Class object; instead, remember
    // just the class name, and dynamically load it per-execution.  This
    // will keep cache memory usage down.
    private final Class rowClass;
    private final ClassLoader statementClassLoader;
    private final RelDataType rowType;
    private final Method method;
    private final String xmiFennelPlan;
    private final Set referencedObjectIds;
    private final Map<String, RelDataType> resultSetTypeMap;

    //~ Constructors -----------------------------------------------------------

    FarragoExecutableJavaStmt(
        File packageDir,
        Class rowClass,
        ClassLoader statementClassLoader,
        RelDataType preparedRowType,
        RelDataType dynamicParamRowType,
        Method method,
        String xmiFennelPlan,
        boolean isDml,
        Set referencedObjectIds,
        TableAccessMap tableAccessMap,
        Map<String, RelDataType> resultSetTypeMap)
    {
        super(dynamicParamRowType, isDml, tableAccessMap);

        this.packageDir = packageDir;
        this.rowClass = rowClass;
        this.statementClassLoader = statementClassLoader;
        this.method = method;
        this.xmiFennelPlan = xmiFennelPlan;
        this.referencedObjectIds = referencedObjectIds;
        this.resultSetTypeMap = resultSetTypeMap;

        rowType = preparedRowType;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionExecutableStmt
    public RelDataType getRowType()
    {
        return rowType;
    }

    // implement FarragoSessionExecutableStmt
    public Set getReferencedObjectIds()
    {
        return referencedObjectIds;
    }

    // implement FarragoSessionExecutableStmt
    public ResultSet execute(FarragoSessionRuntimeContext runtimeContext)
    {
        try {
            runtimeContext.setStatementClassLoader(statementClassLoader);

            if (xmiFennelPlan != null) {
                runtimeContext.loadFennelPlan(xmiFennelPlan);
            }

            // NOTE jvs 1-May-2004: This sequence is subtle.  We can't open all
            // Fennel tuple streams yet, since some may take Java streams as
            // input, and the Java streams are created by method.invoke below
            // (which calls the generated execute method to obtain an iterator).
            //  This means that the generated execute must NOT try to prefetch
            // any data, since the Fennel streams aren't open yet. In
            // particular, Java iterator implementations must not do prefetch in
            // the constructor (always wait for hasNext/next).
            ResultSet resultSet;
            TupleIter iter =
                (TupleIter) method.invoke(
                    null,
                    new Object[] { runtimeContext });
            resultSet =
                new FarragoTupleIterResultSet(iter,
                    rowClass,
                    rowType,
                    runtimeContext);

            if (xmiFennelPlan != null) {
                // Finally, it's safe to open all streams.
                runtimeContext.openStreams();
            }

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

    // implement FarragoSessionExecutableStmt
    public long getMemoryUsage()
    {
        // TODO: a better approximation.  This only sums the bytecode size of
        // the compiled classes.  Other allocations to estimate are loaded
        // class overhead (e.g. constants and reflection info), type
        // descriptor, JIT code size, and "this" object and fields such as
        // packageDir/referencedObjectIds.
        long nBytes = 0;
        File [] files = packageDir.listFiles();
        for (int i = 0; i < files.length; ++i) {
            if (!files[i].getName().endsWith(".class")) {
                continue;
            }
            nBytes += files[i].length();
        }

        if (xmiFennelPlan != null) {
            nBytes += FarragoUtil.getStringMemoryUsage(xmiFennelPlan);
        }

        return nBytes;
    }

    // implement FarragoSessionExecutableStmt
    public Map<String, RelDataType> getResultSetTypeMap()
    {
        return resultSetTypeMap;
    }
}

// End FarragoExecutableJavaStmt.java
