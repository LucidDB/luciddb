/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// (C) Copyright 2004-2004 Disruptive Tech
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
package net.sf.farrago.trace;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.test.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.db.*;
import net.sf.farrago.util.*;

import java.util.logging.Logger;

/**
 * Contains all of the {@link java.util.logging.Logger tracers} used within
 * Farrago.
 *
 * <p>This class is similar to {@link net.sf.saffron.trace.SaffronTrace}; see
 * there for a description of how to define tracers.
 *
 * @author jhyde
 * @since May 24, 2004
 * @version $Id$
 **/
public class FarragoTrace {
    /**
     * The tracer "net.sf.farrago.catalog.FarragoCatalog"
     * traces {@link FarragoCatalog}.
     */
    public static Logger getCatalogTracer() {
        return getClassTracer(FarragoCatalog.class);
    }

    /**
     * The tracer "net.sf.farrago.cwm.relational.CwmViewImpl"
     * traces {@link CwmViewImpl}.
     */
    public static Logger getCwmViewTracer() {
        return getClassTracer(CwmViewImpl.class);
    }

    /**
     * The tracer "net.sf.farrago.db.FarragoDatabase"
     * traces {@link FarragoDatabase}.
     */
    public static Logger getDatabaseTracer() {
        return getClassTracer(FarragoDatabase.class);
    }

    /**
     * The tracer "net.sf.farrago.db.FarragoDbSession"
     * traces {@link FarragoDbSession}.
     */
    public static Logger getDatabaseSessionTracer() {
        return getClassTracer(FarragoDbSession.class);
    }

    /**
     * The tracer "net.sf.farrago.db.FarragoDbStmtContext"
     * traces {@link FarragoDbStmtContext}.
     */
    public static Logger getDatabaseStatementContextTracer() {
        return getClassTracer(FarragoDbStmtContext.class);
    }

    /**
     * The tracer "net.sf.farrago.ddl.DdlValidator"
     * traces {@link DdlValidator}.
     */
    public static Logger getDdlValidatorTracer() {
        return getClassTracer(DdlValidator.class);
    }

    /**
     * The tracer "net.sf.farrago.fennel.FarragoDbHandle"
     * traces {@link FennelDbHandle}.
     */
    public static Logger getFennelDbHandleTracer() {
        return getClassTracer(FennelDbHandle.class);
    }

    /**
     * The tracer "net.sf.farrago.fennel.FennelStreamGraph"
     * traces {@link FennelStreamGraph}.
     */
    public static Logger getFennelStreamGraphTracer() {
        return getClassTracer(FennelStreamGraph.class);
    }

    /**
     * The tracer "net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver"
     * traces {@link FarragoJdbcEngineDriver}.
     */
    public static Logger getFarragoJdbcEngineDriverTracer() {
        return getClassTracer(FarragoJdbcEngineDriver.class);
    }
    
    /**
     * The tracer "net.sf.farrago.runtime.FarragoIteratorResultSet"
     * traces {@link FarragoIteratorResultSet}.
     */
    public static Logger getFarragoIteratorResultSetTracer() {
        return getClassTracer(FarragoIteratorResultSet.class);
    }
    
    /**
     * The tracer "net.sf.farrago.test.FarragoTestCase"
     * controls tracing during regression tests.
     *
     * @see FarragoTestCase
     */
    public static Logger getTestTracer() {
        return getClassTracer(FarragoTestCase.class);
    }

    /**
     * The tracer "net.sf.farrago.util.FarragoFileAllocation"
     * traces {@link FarragoFileAllocation}.
     */
    public static Logger getFileAllocationTracer() {
        return getClassTracer(FarragoFileAllocation.class);
    }

    /**
     * The tracer "net.sf.farrago.util.FarragoObjectCache"
     * traces {@link FarragoObjectCache}.
     */
    public static Logger getObjectCacheTracer() {
        return getClassTracer(FarragoObjectCache.class);
    }

    /**
     * The tracer "net.sf.farrago.dynamic"
     * controls whether dynamically generated Java code is
     * preserved for debugging (otherwise it is deleted automatically).
     */
    public static Logger getDynamicTracer() {
        return Logger.getLogger("net.sf.farrago.dynamic");
    }

    /**
     * The tracer "net.sf.farrago.query.streamgraph"
     * traces Fennel execution stream graphs when they are
     * constructed.
     */
    public static Logger getPreparedStreamGraphTracer() {
        return Logger.getLogger("net.sf.farrago.query.streamgraph");
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
