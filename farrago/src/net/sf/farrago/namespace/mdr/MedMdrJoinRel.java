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

package net.sf.farrago.namespace.mdr;

import net.sf.saffron.core.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rex.*;
import net.sf.saffron.util.*;
import net.sf.saffron.runtime.*;
import net.sf.saffron.oj.rel.*;
import net.sf.saffron.oj.util.*;
import net.sf.saffron.oj.*;
import net.sf.saffron.oj.stmt.*;

import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.netbeans.api.mdr.*;

import openjava.ptree.*;
import openjava.mop.*;

import java.util.*;
import javax.jmi.model.*;
import javax.jmi.reflect.*;

import java.util.List;

/**
 * MedMdrJoinRel is the relational expression corresponding to
 * a join via association over two MedMdrClassExtents.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMdrJoinRel extends JoinRel
{
    private int leftOrdinal;
    
    private Reference rightReference;
    
    MedMdrJoinRel(
        VolcanoCluster cluster,
        SaffronRel left,
        SaffronRel right,
        RexNode condition,
        int joinType,
        int leftOrdinal,
        Reference rightReference)
    {
        super(cluster,left,right,condition,joinType,Collections.EMPTY_SET);
        // TODO:  support outer joins
        assert(joinType == JoinType.INNER);

        this.leftOrdinal = leftOrdinal;
        this.rightReference = rightReference;
    }

    public Object clone()
    {
        return new MedMdrJoinRel(
            cluster,
            OptUtil.clone(left),
            OptUtil.clone(right),
            RexUtil.clone(condition),
            joinType,
            leftOrdinal,
            rightReference);
    }

    // implement SaffronRel
    public CallingConvention getConvention()
    {
        return CallingConvention.ITERATOR;
    }
    
    // implement SaffronRel
    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        // TODO:  refine
        double rowCount = getRows();
        return planner.makeCost(
            rowCount,
            0,
            rowCount*getRowType().getFieldCount());
    }

    // implement SaffronRel
    public double getRows()
    {
        // REVIEW:  this assumes a one-to-many join
        return right.getRows();
    }

    // TODO:  break up method below
    
    // implement SaffronRel
    public Object implement(RelImplementor implementor,int ordinal)
    {
        assert (ordinal == -1);

        // NOTE:  if you actually want to understand this monster,
        // the best approach is to look at the code it generates
        // (methods getNextRightIterator and calcJoinRow)

        SaffronRel leftRel = getLeft();
        MedMdrClassExtentRel rightRel = (MedMdrClassExtentRel) getRight();
        
        Expression leftChildExp = (Expression)
            implementor.implementChild(this,0,leftRel);

        SaffronType outputRowType = getRowType();
        OJClass outputRowClass = OJUtil.typeToOJClass(outputRowType);

        // first, define class data members
        MemberDeclarationList memberList = new MemberDeclarationList();
        
        Variable varOutputRow = implementor.newVariable();
        FieldDeclaration declOutputRow = new FieldDeclaration(
            new ModifierList(ModifierList.PRIVATE),
            TypeName.forOJClass(outputRowClass),
            varOutputRow.toString(),
            new AllocationExpression(
                outputRowClass,
                new ExpressionList()));
        memberList.add(declOutputRow);

        SaffronType leftRowType = leftRel.getRowType();
        SaffronField [] leftFields = leftRowType.getFields();
        OJClass leftRowClass = OJUtil.typeToOJClass(leftRowType);
        Variable varLeftRow = implementor.newVariable();
        FieldDeclaration declLeftRow = new FieldDeclaration(
            new ModifierList(ModifierList.PRIVATE),
            TypeName.forOJClass(leftRowClass),
            varLeftRow.toString(),
            null);
        memberList.add(declLeftRow);

        MedMdrForeignDataWrapper dataWrapper =
            rightRel.mdrClassExtent.directory.dataWrapper;
        
        Variable varRepository = implementor.newVariable();
        FieldDeclaration declRepository = new FieldDeclaration(
            new ModifierList(ModifierList.PRIVATE),
            TypeName.forClass(MDRepository.class),
            varRepository.toString(),
            null);
        memberList.add(declRepository);

        // construct the body of the getNextRightIterator method
        
        StatementList stmtList = new StatementList();
        MemberDeclaration getNextRightIteratorMethodDecl =
            new MethodDeclaration(
                new ModifierList(ModifierList.PROTECTED),
                TypeName.forClass(Iterator.class),
                "getNextRightIterator",
                new ParameterList(),
                null,
                stmtList);
        memberList.add(getNextRightIteratorMethodDecl);
        
        stmtList.add(
            new ExpressionStatement(
                new AssignmentExpression(
                    varLeftRow,
                    AssignmentExpression.EQUALS,
                    new CastExpression(
                        TypeName.forOJClass(leftRowClass),
                        new FieldAccess("leftObj")))));
            
        Association association = (Association)
            rightReference.getReferencedEnd().getContainer();

        // TODO:  preserve the left type in the FarragoType system instead
        Classifier leftKeyClassifier =
            rightReference.getReferencedEnd().getType();
        RefClass leftKeyRefClass = (RefClass)
            rightRel.getRefObjectFromModelElement(leftKeyClassifier);
        Class leftKeyClass = JmiUtil.getClassForRefClass(leftKeyRefClass);

        boolean useAssocReflection = rightRel.useReflection;

        if (leftKeyClass == RefObject.class) {
            useAssocReflection = true;
        }

        Variable varLeftObj = implementor.newVariable();
        stmtList.add(
            new VariableDeclaration(
                TypeName.forClass(leftKeyClass),
                varLeftObj.toString(),
                new CastExpression(
                    TypeName.forClass(leftKeyClass),
                    new MethodCall(
                        varRepository,
                        "getByMofId",
                        new ExpressionList(
                            new MethodCall(
                                new FieldAccess(
                                    varLeftRow,
                                    leftFields[leftOrdinal].getName()),
                                "toString",
                                new ExpressionList()))))));
        
        Expression collectionExpr = null;
        if (!useAssocReflection) {
            String accessorName = JmiUtil.getAccessorName(
                rightReference.getExposedEnd());
            try {
                // verify that the desired accessor method actually exists
                // using Java reflection, which will throw if it
                // doesn't exist
                leftKeyClass.getMethod(accessorName,new Class[0]);

                // all good:  generate the call
                collectionExpr =
                    new MethodCall(
                        varLeftObj,
                        accessorName,
                        new ExpressionList());
            } catch (Exception ex) {
                // oops, the necessary accessor wasn't generated; that's
                // OK, we can fall back to using JMI reflection
                useAssocReflection = true;
            }
        }

        Variable varRefAssociation = null;
        if (useAssocReflection) {
            varRefAssociation = implementor.newVariable();
            FieldDeclaration declRefAssociation = new FieldDeclaration(
                new ModifierList(ModifierList.PRIVATE),
                TypeName.forClass(RefAssociation.class),
                varRefAssociation.toString(),
                null);
            memberList.add(declRefAssociation);

            // generate the JMI reflective query call
            collectionExpr =
                new MethodCall(
                    varRefAssociation,
                    "refQuery",
                    new ExpressionList(
                        Literal.makeLiteral(
                            rightReference.getReferencedEnd().getName()),
                        varLeftObj));
        }
        
        stmtList.add(
            new ReturnStatement(
                new MethodCall(
                    collectionExpr,
                    "iterator",
                    new ExpressionList())));

        // construct the body of the calcJoinRow method
        
        stmtList = new StatementList();
        MemberDeclaration calcJoinRowMethodDecl =
            new MethodDeclaration(
                new ModifierList(ModifierList.PROTECTED),
                TypeName.forClass(Object.class),
                "calcJoinRow",
                new ParameterList(),
                null,
                stmtList);
        memberList.add(calcJoinRowMethodDecl);

        Variable varRightClassifier = null;

        if (!rightReference.getType().equals(
                rightRel.mdrClassExtent.refClass.refMetaObject()))
        {
            // since the right-hand input is more specific than the
            // corresponding association end, we have to filter out any
            // unrelated types we might encounter
            Expression instanceofExpr;
            if (rightRel.useReflection) {
                varRightClassifier = implementor.newVariable();
                FieldDeclaration declRightClassifier = new FieldDeclaration(
                    new ModifierList(ModifierList.PRIVATE),
                    TypeName.forClass(RefObject.class),
                    varRightClassifier.toString(),
                    null);
                memberList.add(declRightClassifier);

                instanceofExpr =
                    new MethodCall(
                        new CastExpression(
                            OJClass.forClass(RefObject.class),
                            new FieldAccess("rightObj")),
                        "refIsInstanceOf",
                        new ExpressionList(
                            varRightClassifier,
                            Literal.constantTrue()));
            } else {
                instanceofExpr = 
                    new InstanceofExpression(
                        new FieldAccess("rightObj"),
                        TypeName.forClass(rightRel.rowClass));
            }
            stmtList.add(
                new IfStatement(
                    new UnaryExpression(
                        UnaryExpression.NOT,
                        instanceofExpr),
                    new StatementList(
                        new ReturnStatement(
                            Literal.constantNull()))));
        }

        RexNode [] rightExps = rightRel.implementProjection(
            new FieldAccess("rightObj"));

        SaffronField [] fields = outputRowType.getFields();
        int nLeft = leftRowType.getFieldCount();
        int n = outputRowType.getFieldCount();
        for (int i = 0; i < n; i++) {
            Expression lhs = new FieldAccess(varOutputRow,fields[i].getName());
            if (i < nLeft) {
                // REVIEW:  is this assignment-by-reference for object types
                // OK?  If it is, we should be generating it in such
                // a way that it only gets executed once.
                stmtList.add(
                    new ExpressionStatement(
                        new AssignmentExpression(
                            lhs,
                            AssignmentExpression.EQUALS,
                            new FieldAccess(
                                varLeftRow,
                                leftFields[i].getName()))));
            } else {
                RexNode rhs = rightExps[i - nLeft];
                implementor.translateAssignment(
                    this,
                    fields[i].getType(),
                    lhs,
                    rhs,
                    stmtList,
                    memberList);
            }
        }
        stmtList.add(new ReturnStatement(varOutputRow));

        // construct the open method
        
        stmtList = new StatementList();
        MemberDeclaration openMethodDecl =
            new MethodDeclaration(
                new ModifierList(ModifierList.PROTECTED),
                TypeName.forOJClass(OJSystem.VOID),
                "open",
                new ParameterList(),
                null,
                stmtList);
        memberList.add(openMethodDecl);
        
        stmtList.add(
            new ExpressionStatement(
                new AssignmentExpression(
                    varRepository,
                    AssignmentExpression.EQUALS,
                    new CastExpression(
                        TypeName.forClass(MDRepository.class),
                        dataWrapper.generateRuntimeSupportCall(
                            Literal.constantNull())))));
        if (varRefAssociation != null) {
            stmtList.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        varRefAssociation,
                        AssignmentExpression.EQUALS,
                        new CastExpression(
                            OJClass.forClass(RefAssociation.class),
                            rightRel.getRefBaseObjectRuntimeExpression(
                                association)))));
        }
        if (varRightClassifier != null) {
            stmtList.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        varRightClassifier,
                        AssignmentExpression.EQUALS,
                        new MethodCall(
                            new CastExpression(
                                OJClass.forClass(RefClass.class),
                                rightRel.getRefBaseObjectRuntimeExpression(
                                    rightRel.mdrClassExtent.refClass)),
                            "refMetaObject",
                            new ExpressionList()))));
        }
        
        // put it all together in an anonymous class definition
        
        Expression newIteratorExp = new AllocationExpression(
            TypeName.forClass(NestedLoopCalcIterator.class),
            new ExpressionList(leftChildExp),
            memberList);

        return newIteratorExp;
    }
}

// End MedMdrJoinRel.java
