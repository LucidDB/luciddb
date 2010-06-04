/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
import java.util.logging.*;

import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.runtime.*;
import org.eigenbase.util.*;


/**
 * FarragoExecutableJavaStmt implements FarragoSessionExecutableStmt via a
 * compiled Java class. It extends upon FarragoExecutableFennelStmt, which
 * implements the Fennel portion of a statement.
 *
 * <p>NOTE: be sure to read superclass warnings before modifying this class.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoExecutableJavaStmt
    extends FarragoExecutableFennelStmt
{
    //~ Instance fields --------------------------------------------------------

    private final File packageDir;

    // TODO jvs 4-June-2004:  don't pin a Class object; instead, remember
    // just the class name, and dynamically load it per-execution.  This
    // will keep cache memory usage down.
    private final Class rowClass;
    private final ClassLoader stmtClassLoader;
    private final Method stmtMethod;
    private final List<FarragoTransformDef> transformDefs;
    private final Map<String, RelDataType> resultSetTypeMap;
    private final Map<String, RelDataType> iterCalcTypeMap;
    private final int totalByteCodeSize;

    //~ Constructors -----------------------------------------------------------

    FarragoExecutableJavaStmt(
        File packageDir,
        Class rowClass,
        ClassLoader stmtClassLoader,
        RelDataType preparedRowType,
        List<List<String>> fieldOrigins,
        RelDataType dynamicParamRowType,
        Method stmtMethod,
        List<FarragoTransformDef> transformDefs,
        String xmiFennelPlan,
        boolean isDml,
        TableModificationRel.Operation tableModOp,
        Map<String, String> referencedObjectTimestampMap,
        TableAccessMap tableAccessMap,
        Map<String, RelDataType> resultSetTypeMap,
        Map<String, RelDataType> iterCalcTypeMap,
        int totalByteCodeSize)
    {
        super(
            preparedRowType,
            fieldOrigins,
            dynamicParamRowType,
            xmiFennelPlan,
            null,
            isDml,
            tableModOp,
            referencedObjectTimestampMap,
            tableAccessMap,
            resultSetTypeMap);

        this.packageDir = packageDir;
        this.rowClass = rowClass;
        this.stmtClassLoader = stmtClassLoader;
        this.stmtMethod = stmtMethod;
        this.transformDefs = transformDefs;
        this.resultSetTypeMap = resultSetTypeMap;
        this.iterCalcTypeMap = iterCalcTypeMap;
        this.totalByteCodeSize = totalByteCodeSize;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionExecutableStmt
    public ResultSet execute(FarragoSessionRuntimeContext runtimeContext)
    {
        try {
            runtimeContext.setStatementClassLoader(stmtClassLoader);

            if (xmiFennelPlan != null) {
                runtimeContext.loadFennelPlan(xmiFennelPlan);
            }

            // NOTE jvs 1-May-2004: This sequence is subtle.  We can't open all
            // Fennel tuple streams yet, since some may take Java streams as
            // input, and the Java streams are created by stmtMethod.invoke
            // below (which calls the generated execute stmtMethod to obtain an
            // iterator). This means that the generated execute must NOT try to
            // prefetch any data, since the Fennel streams aren't open yet. In
            // particular, Java iterator implementations must not do prefetch in
            // the constructor (always wait for hasNext/next).
            TupleIter iter =
                (TupleIter) stmtMethod.invoke(
                    null,
                    new Object[] { runtimeContext });

            FarragoTupleIterResultSet resultSet =
                new FarragoTupleIterResultSet(
                    iter,
                    rowClass,
                    rowType,
                    fieldOrigins,
                    runtimeContext,
                    null);

            // instantiate and initialize all generated FarragoTransforms.
            for (FarragoTransformDef tdef : transformDefs) {
                tdef.init(runtimeContext);
            }

            if (xmiFennelPlan != null) {
                // Finally, it's safe to open all streams.
                runtimeContext.openStreams();
            }

            runtimeContext = null;
            resultSet.setOpened();

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
        // The size of the Java portion of the statement is estimated based on
        // the bytecode size times an additional factor of .75. That factor was
        // derived from measurements capturing the relative size of JIT code
        // versus bytecode size.  JIT code size relative to bytecode size varied
        // from .25 to .5.  So, we use .5 to account for the JIT code and then
        // add an additional .25 factor for other class overhead (e.g. constants
        // and reflection info), type descriptor, and "this" object and fields
        // such as packageDir/referencedObjectIds.
        long nBytes = (long) ((double) totalByteCodeSize * 1.75);

        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine("Java bytecode size = " + totalByteCodeSize + " bytes");
            if (xmiFennelPlan != null) {
                int xmiSize = FarragoUtil.getStringMemoryUsage(xmiFennelPlan);
                tracer.fine("XMI Fennel plan size = " + xmiSize + " bytes");
            }
        }

        // call the superclass to account for the Fennel XMI plan
        if (xmiFennelPlan != null) {
            nBytes += super.getMemoryUsage();
        }

        return nBytes;
    }

    // implement FarragoSessionExecutableStmt
    public Map<String, RelDataType> getResultSetTypeMap()
    {
        return resultSetTypeMap;
    }

    // implement FarragoSessionExecutableStmt
    public Map<String, RelDataType> getIterCalcTypeMap()
    {
        return iterCalcTypeMap;
    }
}

// End FarragoExecutableJavaStmt.java
