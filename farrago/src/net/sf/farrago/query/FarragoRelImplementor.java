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

import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rex.*;
import net.sf.saffron.core.*;
import net.sf.saffron.oj.util.*;
import net.sf.farrago.type.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.util.*;
import net.sf.farrago.fem.fennel.*;
import openjava.mop.*;
import openjava.ptree.*;

import java.util.*;

import java.util.List;

/**
 * FarragoRelImplementor refines RelImplementor with some Farrago-specifics.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRelImplementor extends RelImplementor
{
    FarragoPreparingStmt preparingStmt;
    OJClass ojAssignableValue;
    OJClass ojBytePointer;
    OJClass ojNullablePrimitive;

    // TODO jvs 12-Feb-2004:  get rid of stack once we're capable of building
    // up an entire disconnected graph
    private List streamDefSetStack;
    
    public FarragoRelImplementor(
        FarragoPreparingStmt preparingStmt,
        RexBuilder rexBuilder)
    {
        super(rexBuilder);

        this.preparingStmt = preparingStmt;
        ojAssignableValue = OJClass.forClass(AssignableValue.class);
        ojBytePointer = OJClass.forClass(BytePointer.class);
        ojNullablePrimitive = OJClass.forClass(NullablePrimitive.class);

        streamDefSetStack = new ArrayList();
    }

    void pushStreamDefSet()
    {
        streamDefSetStack.add(new HashSet());
    }

    Set popStreamDefSet()
    {
        return (Set)
            (streamDefSetStack.remove(streamDefSetStack.size() - 1));
    }

    private Set getStreamDefSet()
    {
        return (Set)
            streamDefSetStack.get(streamDefSetStack.size() - 1);
    }

    public FemExecutionStreamDef implementFennelRel(SaffronRel rel)
    {
        FemExecutionStreamDef streamDef =
            ((FennelRel) rel).toStreamDef(this);
        registerStreamDef(streamDef,rel);
        return streamDef;
    }

    private void registerStreamDef(
        FemExecutionStreamDef streamDef,
        SaffronRel rel)
    {
        if (streamDef.getName() != null) {
            // already registered
            return;
        }
        if (rel != null) {
            // correlate stream name with rel which produced it
            streamDef.setName(rel.getRelTypeName() + "#" + rel.getId());
        } else {
            // TODO jvs 12-Feb-2004:  need a better UUID for
            // anonymous stream names
            
            // anonymous stream
            streamDef.setName(
                ReflectUtil.getUnqualifiedClassName(streamDef.getClass())
                + "#" + System.identityHashCode(streamDef));
        }
        getStreamDefSet().add(streamDef);

        // recursively ensure all inputs have also been registered
        Iterator iter = streamDef.getInput().iterator();
        while (iter.hasNext()) {
            registerStreamDef(
                (FemExecutionStreamDef) iter.next(),
                null);
        }
    }

    // override RelImplementor
    protected Translator newTranslator(
        RelImplementor relImplementor,SaffronRel rel)
    {
        assert(this == relImplementor);
        // REVIEW:  should probably just fail here
        return new FarragoRexToJavaTranslator(this,rel,null,null);
    }

    // override RelImplementor
    public Expression translateViaStatements(
        SaffronRel rel,
        RexNode exp,
        StatementList stmtList,
        MemberDeclarationList memberList)
    {
        FarragoRexToJavaTranslator translator = new FarragoRexToJavaTranslator(
            this,rel,stmtList,memberList);
        return translator.go(exp);
    }
    
    // override RelImplementor
    public void translateAssignment(
        SaffronRel rel,
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
