/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rex.*;
import net.sf.saffron.core.*;
import net.sf.saffron.oj.util.*;
import net.sf.saffron.oj.rel.*;
import net.sf.farrago.type.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.type.runtime.*;
import net.sf.farrago.util.*;
import net.sf.farrago.fem.fennel.*;
import openjava.mop.*;
import openjava.ptree.*;

import java.util.*;

/**
 * FarragoRelImplementor refines {@link JavaRelImplementor} with some Farrago
 * specifics.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRelImplementor extends JavaRelImplementor
        implements FennelRelImplementor
{
    FarragoPreparingStmt preparingStmt;
    OJClass ojAssignableValue;
    OJClass ojBytePointer;
    OJClass ojNullablePrimitive;

    private Set streamDefSet;
    
    public FarragoRelImplementor(
        FarragoPreparingStmt preparingStmt,
        RexBuilder rexBuilder)
    {
        super(rexBuilder);

        this.preparingStmt = preparingStmt;
        ojAssignableValue = OJClass.forClass(AssignableValue.class);
        ojBytePointer = OJClass.forClass(BytePointer.class);
        ojNullablePrimitive = OJClass.forClass(NullablePrimitive.class);

        streamDefSet = new HashSet();
    }

    // implement FennelRelImplementor
    public FemExecutionStreamDef visitFennelChild(FennelRel rel)
    {
        FemExecutionStreamDef streamDef = rel.toStreamDef(this);
        registerRelStreamDef(streamDef,rel);
        return streamDef;
    }

    /**
     * Override method to deal with the possibility that we are being called
     * from a {@link FennelRel} via our {@link FennelRelImplementor}
     * interface.
     */ 
    public Object visitChildInternal(SaffronRel child) {
        if (child instanceof FennelRel) {
            return ((FennelRel) child).implementFennelChild(this);
        }
        return super.visitChildInternal(child);
    }

    public Set getStreamDefSet()
    {
        return streamDefSet;
    }

    // implement FennelRelImplementor
    public void registerRelStreamDef(
        FemExecutionStreamDef streamDef,
        SaffronRel rel)
    {
        registerStreamDef(streamDef,rel,rel.getRowType());
    }

    private void registerStreamDef(
        FemExecutionStreamDef streamDef,
        SaffronRel rel,
        SaffronType rowType)
    {
        if (streamDef.getName() != null) {
            // already registered
            return;
        }

        String streamName = getStreamGlobalName(streamDef,rel);
        streamDef.setName(streamName);
        streamDefSet.add(streamDef);

        if (streamDef.getOutputDesc() == null) {
            streamDef.setOutputDesc(
                FennelRelUtil.createTupleDescriptorFromRowType(
                    preparingStmt.getCatalog(),
                    rowType));
        }

        // recursively ensure all inputs have also been registered
        Iterator iter = streamDef.getInput().iterator();
        while (iter.hasNext()) {
            registerStreamDef(
                (FemExecutionStreamDef) iter.next(),
                null,
                rowType);
        }
    }

    /**
     * Construct a globally unique name for an execution stream.  This name is
     * used to label and find C++ ExecutionStreams.
     *
     * @param streamDef stream definition
     *
     * @param rel rel which generated stream definition, or null if none
     *
     * @return global name for stream
     */
    public String getStreamGlobalName(
        FemExecutionStreamDef streamDef,
        SaffronRel rel)
    {
        String streamName;
        if (rel != null) {
            // correlate stream name with rel which produced it
            streamName = rel.getRelTypeName() + "#" + rel.getId();
        } else {
            // anonymous stream
            streamName = 
                ReflectUtil.getUnqualifiedClassName(streamDef.getClass());
        }
        // make sure stream names are globally unique
        streamName = streamName + ":" + JmiUtil.getObjectId(streamDef);
        return streamName;
    }

    // override JavaRelImplementor
    protected RexToJavaTranslator newTranslator(SaffronRel rel)
    {
        // REVIEW:  should probably just fail here
        return new FarragoRexToJavaTranslator(this,rel,null,null);
    }

    // override JavaRelImplementor
    public Expression translateViaStatements(
        JavaRel rel,
        RexNode exp,
        StatementList stmtList,
        MemberDeclarationList memberList)
    {
        FarragoRexToJavaTranslator translator = new FarragoRexToJavaTranslator(
            this,rel,stmtList,memberList);
        return translator.go(exp);
    }
    
    // override JavaRelImplementor
    public void translateAssignment(
        JavaRel rel,
        SaffronType lhsType,
        Expression lhsExp,
        RexNode rhs,
        StatementList stmtList,
        MemberDeclarationList memberList)
    {
        FarragoRexToJavaTranslator translator = new FarragoRexToJavaTranslator(
            this,rel,stmtList,memberList);
        Expression rhsExp = translator.go(rhs);
        translator.convertCastOrAssignment(
            lhsType,
            rhs.getType(),
            lhsExp,
            rhsExp);
    }

}

// End FarragoRelImplementor.java
