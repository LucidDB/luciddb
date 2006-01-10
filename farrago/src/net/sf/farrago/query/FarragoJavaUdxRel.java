/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.rex.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.util.*;

import net.sf.farrago.runtime.*;

/**
 * FarragoJavaUdxRel is the implementation for a {@link
 * TableFunctionRel} which invokes a Java UDX (user-defined transformation).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJavaUdxRel extends TableFunctionRelBase
    implements JavaRel
{
    /**
     * Creates a <code>FarragoJavaUdxRel</code>.
     *
     * @param cluster {@link RelOptCluster} this relational expression
     *        belongs to
     *
     * @param rexCall function invocation expression
     *
     * @param rowType row type produced by function
     */
    public FarragoJavaUdxRel(
        RelOptCluster cluster, RexNode rexCall, RelDataType rowType)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.ITERATOR),
            rexCall,
            rowType);
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO jvs 8-Jan-2006:  get estimate from user or history
        return planner.makeTinyCost();
    }

    // implement JavaRel
    public ParseTree implement(JavaRelImplementor implementor)
    {
        final RelDataType outputRowType = getRowType();
        OJClass outputRowClass = OJUtil.typeToOJClass(
            outputRowType,
            implementor.getTypeFactory());

        MemberDeclarationList memberList = new MemberDeclarationList();
        
        StatementList executeMethodBody = new StatementList();
        implementor.translateViaStatements(
            this,
            getCall(),
            executeMethodBody,
            memberList);

        MemberDeclaration executeMethodDecl =
            new MethodDeclaration(new ModifierList(ModifierList.PROTECTED),
                TypeName.forOJClass(OJSystem.VOID), "executeUdx",
                new ParameterList(), null, executeMethodBody);
        memberList.add(executeMethodDecl);

        Expression iterExp =
            new AllocationExpression(
                OJUtil.typeNameForClass(FarragoJavaUdxIterator.class),
                new ExpressionList(
                    new ClassLiteral(TypeName.forOJClass(outputRowClass))),
                memberList);

        return iterExp;
    }
}

// End FarragoJavaUdxRel.java
