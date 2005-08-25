/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
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
package net.sf.farrago.trace;

import com.disruptivetech.farrago.calc.*;

import java.util.logging.Logger;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.db.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.test.*;
import net.sf.farrago.util.*;


/**
 * Contains all of the {@link java.util.logging.Logger tracers} used within
 * Farrago.
 *
 * <p>This class is similar to {@link org.eigenbase.trace.EigenbaseTrace}; see
 * there for a description of how to define tracers.
 *
 * @author jhyde
 * @since May 24, 2004
 * @version $Id$
 **/
public abstract class FarragoTrace
{
    //~ Methods ---------------------------------------------------------------

    /**
     * The tracer "net.sf.farrago.catalog.FarragoRepos"
     * traces {@link FarragoRepos}.
     */
    public static Logger getReposTracer()
    {
        return getClassTracer(FarragoRepos.class);
    }

    /**
     * The tracer "net.sf.farrago.db.FarragoDatabase"
     * traces {@link FarragoDatabase}.
     */
    public static Logger getDatabaseTracer()
    {
        return getClassTracer(FarragoDatabase.class);
    }

    /**
     * The tracer "net.sf.farrago.db.FarragoDbSession"
     * traces {@link FarragoDbSession}.
     */
    public static Logger getDatabaseSessionTracer()
    {
        return getClassTracer(FarragoDbSession.class);
    }

    /**
     * The tracer "net.sf.farrago.db.FarragoDbStmtContext"
     * traces {@link FarragoDbStmtContext}.
     */
    public static Logger getDatabaseStatementContextTracer()
    {
        return getClassTracer(FarragoDbStmtContext.class);
    }

    /**
     * The tracer "net.sf.farrago.ddl.DdlValidator"
     * traces {@link DdlValidator}.
     */
    public static Logger getDdlValidatorTracer()
    {
        return getClassTracer(DdlValidator.class);
    }

    /**
     * The tracer "net.sf.farrago.fennel.FarragoDbHandle"
     * traces {@link FennelDbHandle}.
     */
    public static Logger getFennelDbHandleTracer()
    {
        return getClassTracer(FennelDbHandle.class);
    }

    /**
     * The tracer "net.sf.farrago.fennel.FennelStreamGraph"
     * traces {@link FennelStreamGraph}.
     */
    public static Logger getFennelStreamGraphTracer()
    {
        return getClassTracer(FennelStreamGraph.class);
    }

    /**
     * The tracer "net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver"
     * traces {@link FarragoJdbcEngineDriver}.
     */
    public static Logger getFarragoJdbcEngineDriverTracer()
    {
        return getClassTracer(FarragoJdbcEngineDriver.class);
    }

    /**
     * The tracer "net.sf.farrago.runtime.FarragoIteratorResultSet"
     * traces {@link FarragoIteratorResultSet}.
     */
    public static Logger getFarragoIteratorResultSetTracer()
    {
        return getClassTracer(FarragoIteratorResultSet.class);
    }

    /**
     * The tracer "net.sf.farrago.test.FarragoTestCase"
     * controls tracing during regression tests.
     *
     * @see FarragoTestCase
     */
    public static Logger getTestTracer()
    {
        return getClassTracer(FarragoTestCase.class);
    }

    /**
     * The tracer "net.sf.farrago.util.FarragoFileAllocation"
     * traces {@link FarragoFileAllocation}.
     */
    public static Logger getFileAllocationTracer()
    {
        return getClassTracer(FarragoFileAllocation.class);
    }

    /**
     * The tracer "net.sf.farrago.util.FarragoFileLockAllocation"
     * traces {@link FarragoFileLockAllocation}.
     */
    public static Logger getFileLockAllocationTracer()
    {
        return getClassTracer(FarragoFileLockAllocation.class);
    }

    /**
     * The tracer "net.sf.farrago.util.FarragoObjectCache"
     * traces {@link FarragoObjectCache}.
     */
    public static Logger getObjectCacheTracer()
    {
        return getClassTracer(FarragoObjectCache.class);
    }

    /**
     * The tracer "net.sf.farrago.dynamic"
     * controls whether dynamically generated Java code is
     * preserved for debugging (otherwise it is deleted automatically).
     */
    public static Logger getDynamicTracer()
    {
        return Logger.getLogger("net.sf.farrago.dynamic");
    }

    /**
     * The tracer "net.sf.farrago.query.streamgraph"
     * traces Fennel execution stream graphs when they are
     * constructed.
     */
    public static Logger getPreparedStreamGraphTracer()
    {
        return Logger.getLogger("net.sf.farrago.query.streamgraph");
    }

    /**
     * The tracer "net.sf.farrago.query.plandump"
     * cause the plan to be dumped before and after optimization.
     */
    public static Logger getPlanDumpTracer()
    {
        return Logger.getLogger("net.sf.farrago.query.plandump");
    }

    /**
     * The tracer "net.sf.farrago.plannerviz" controls JGraph visualization of
     * planner activity.  Planner visualization is requested with a DDL command
     * sequence like:
     *
     *<pre><code>
     *
     * create jar sys_boot.sys_boot.plannerviz_plugin
     * library 'file:examples/plannerviz/plugin/FarragoPlannerviz.jar'
     * options(0);
     *
     * alter session implementation set jar sys_boot.sys_boot.plannerviz_plugin;
     *
     *</code></pre>
     *
     * The CREATE command installs the plugin (change the jar library path as
     * appropriate) into the SYS_BOOT schema.  The ALTER command puts the plugin
     * into effect for the current session; this should not be done unless
     * Farrago is running from a single-session interactive console, because a
     * new GUI window will pop up on the display where the server is running
     * each time a query is executed.  The plugin can be disabled with:
     *
     *<pre><code>
     *
     * alter session implementation set default;
     *
     *</code></pre>
     *
     *<p>
     *
     * The behavior of the plugin can be controlled via this trace
     * setting:
     *
     * <ol>
     *
     * <li>{@link java.util.logging.Level#FINE}:  render only logical
     * equivalence classes
     *
     * <li>{@link java.util.logging.Level#FINER}:  render only physical
     * equivalence classes
     *
     * <li>{@link java.util.logging.Level#FINEST}:  render both logical
     * and physical equivalence classes
     *
     * </ol>
     */
    public static Logger getPlannerVizTracer()
    {
        return Logger.getLogger("net.sf.farrago.plannerviz");
    }

    /**
     * The tracer "net.sf.farrago.query.rule" traces Farrago's custom
     * optimizer rules.
     */
    public static Logger getOptimizerRuleTracer()
    {
        return Logger.getLogger("net.sf.farrago.query.rule");
    }

    /**
     * The tracer "net.sf.farrago.mdr" traces Farrago's use of
     * MDR.
     */
    public static Logger getMdrTracer()
    {
        return Logger.getLogger("net.sf.farrago.mdr");
    }

    /**
     * The "com.disruptivetech.farrago.calc.CalcProgramBuilder" tracer prints
     * the generated program at level {@link java.util.logging.Level#FINE} or
     * higher.
     */
    public static Logger getCalcTracer()
    {
        return Logger.getLogger(CalcProgramBuilder.class.getName());
    }

    /**
     * Gets the logger to be used for tracing a particular class.
     *
     * @param clazz the class to trace
     *
     * @return appropriate Logger instance
     */
    public static Logger getClassTracer(Class clazz)
    {
        return Logger.getLogger(clazz.getName());
    }
}


// End FarragoTrace.java
