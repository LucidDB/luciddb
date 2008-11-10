/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2008 The Eigenbase Project
// Copyright (C) 2003-2008 Disruptive Tech
// Copyright (C) 2005-2008 LucidEra, Inc.
// Portions Copyright (C) 2003-2008 John V. Sichi
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

import java.lang.reflect.Constructor;
import java.util.logging.*;

import openjava.ptree.*;

import org.eigenbase.rel.RelNode;
import org.eigenbase.util.Util;
import net.sf.farrago.fennel.FennelStreamGraph;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.FarragoSessionRuntimeContext;
import net.sf.farrago.trace.FarragoTrace;

/**
 * Defines a {@link net.sf.farrago.runtime.FarragoTransform}, the java peer of a
 * fennel::JavaTransformExecStream. A FarragoTransformDef is constructed by a
 * FarragoRelImplementor and then handed off to a FarragoExecutableJavaStmt, which
 * instantiates the FarragoTransform.
 *
 * @author Marc Berkowitz
 */
public class FarragoTransformDef
{
    // trace with fennel plan
    private static final Logger tracer =
        FarragoTrace.getPreparedStreamGraphTracer();

    private RelNode relNode;
    private ClassDeclaration sourceCode;
    private Class objectCode;
    private String className;
    private String streamName;
    private FarragoTransform.InputBinding[] inputBindings;

    FarragoTransformDef(RelNode relNode, ClassDeclaration sourceCode)
    {
        this.relNode = relNode;
        this.sourceCode = sourceCode;
    }

    public String toString()
    {
        return "[FarragoTransformDef" +
            " rel: " + relNode +
            " class:" + className +
            " stream:" + streamName +
            "]";
    }

    void setStreamName(String s)
    {
        this.streamName = s;
    }

    String getClassName()
    {
        return className;
    }

    // called when the implementor gives the def to an executable stmt
    void disconnectFromImplementor()
    {
        relNode = null;
    }

    void compile(FarragoPreparingStmt stmt, String pkgName)
    {
        if (tracer.isLoggable(Level.FINEST)) {
            tracer.finest("compiling " + this);
        }
        CompilationUnit compUnit =
            new CompilationUnit(
                pkgName,
                new String[0],
                new ClassDeclarationList(sourceCode));
        objectCode =
            stmt.compileClass(
                pkgName,
                sourceCode.getName(),
                compUnit.toString());
        className = objectCode.getName();
    }

    private void bindInputs(FarragoRuntimeContext conn)
    {
        FennelStreamGraph graph = conn.getFennelStreamGraph();
        assert (streamName != null);
        String[] inputStreams = graph.getInputStreams(streamName);
        int n = inputStreams.length;
        this.inputBindings = new FarragoTransform.InputBinding[n];
        for (int i = 0; i < n; i++) {
            this.inputBindings[i] =
                new FarragoTransform.InputBinding(inputStreams[i], i);
        }
    }

    public void init(FarragoSessionRuntimeContext fsrc) throws Error
    {
        if (tracer.isLoggable(Level.FINEST)) {
            tracer.finest("init " + this);
        }
        try {
            FarragoRuntimeContext conn = (FarragoRuntimeContext) fsrc;
            bindInputs(conn);

            Constructor<FarragoTransform> cons =
                objectCode.getConstructor(new Class[0]);
            FarragoTransform t = cons.newInstance(new Object[0]);

            assert t != null;
            assert className != null;
            conn.registerFarragoTransform(className, t);

            assert streamName != null;
            assert inputBindings != null;
            t.init(conn, streamName, inputBindings);
        } catch (Exception e) {
            throw Util.newInternal(e);
        }
    }
}

// End FarragoTransformDef.java
