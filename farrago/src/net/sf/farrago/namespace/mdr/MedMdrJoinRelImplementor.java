/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package net.sf.farrago.namespace.mdr;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import net.sf.farrago.util.*;
import net.sf.farrago.query.*;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.rex.RexToOJTranslator;
import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.runtime.*;
import org.eigenbase.util.*;
import org.netbeans.api.mdr.*;


/**
 * MedMdrJoinRelImplementor keeps track of lots of transient state
 * needed for the MedMdrJoinRel.implement() call.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMdrJoinRelImplementor
{
    //~ Instance fields -------------------------------------------------------

    private FarragoRelImplementor implementor;
    private StatementList stmtList;
    private MedMdrJoinRel joinRel;
    private RelNode leftRel;
    private MedMdrClassExtentRel rightRel;
    private Expression leftChildExp;
    private RelDataType outputRowType;
    private OJClass outputRowClass;
    private MemberDeclarationList memberList;
    private Variable varOutputRow;
    private RelDataType leftRowType;
    private RelDataTypeField [] leftFields;
    private OJClass leftRowClass;
    private Variable varLeftRow;
    private MedMdrDataServer server;
    private Variable varRepository;
    private Variable varRightClassifier;
    private Association association;
    private Classifier leftKeyClassifier;
    private RefClass leftKeyRefClass;
    private Class leftKeyClass;
    private Variable varRefAssociation;

    //~ Constructors ----------------------------------------------------------

    MedMdrJoinRelImplementor(MedMdrJoinRel joinRel)
    {
        this.joinRel = joinRel;
    }

    //~ Methods ---------------------------------------------------------------

    Expression implement(JavaRelImplementor implementor)
    {
        // NOTE:  if you actually want to understand this monster,
        // the best approach is to look at the code it generates
        // (particularly methods getNextRightIterator and calcJoinRow)
        this.implementor = (FarragoRelImplementor) implementor;

        leftRel = joinRel.getLeft();
        rightRel = (MedMdrClassExtentRel) joinRel.getRight();

        leftChildExp =
            implementor.visitJavaChild(joinRel, 0, (JavaRel) leftRel);

        outputRowType = joinRel.getRowType();
        outputRowClass = OJUtil.typeToOJClass(
            outputRowType,
            implementor.getTypeFactory());

        // first, define class data members
        memberList = new MemberDeclarationList();
        generateRequiredMembers();

        // construct the body of the getNextRightIterator method
        generateGetNextRightIterator();

        // construct the body of the calcJoinRow method
        generateCalcJoinRow();

        // construct the open method
        generateOpen();

        // put it all together in an anonymous class definition
        Expression newIteratorExp =
            new AllocationExpression(
                OJUtil.typeNameForClass(NestedLoopCalcIterator.class),
                new ExpressionList(
                    leftChildExp,
                    Literal.makeLiteral(
                        joinRel.getJoinType() == JoinRelType.LEFT)),
                memberList);

        return newIteratorExp;
    }

    private void generateRequiredMembers()
    {
        varOutputRow = implementor.newVariable();
        FieldDeclaration declOutputRow =
            new FieldDeclaration(new ModifierList(ModifierList.PRIVATE),
                TypeName.forOJClass(outputRowClass),
                varOutputRow.toString(),
                new AllocationExpression(
                    outputRowClass,
                    new ExpressionList()));
        memberList.add(declOutputRow);

        leftRowType = leftRel.getRowType();
        leftFields = leftRowType.getFields();
        leftRowClass = OJUtil.typeToOJClass(
            leftRowType, 
            implementor.getTypeFactory());
        varLeftRow = implementor.newVariable();
        FieldDeclaration declLeftRow =
            new FieldDeclaration(new ModifierList(ModifierList.PRIVATE),
                TypeName.forOJClass(leftRowClass),
                varLeftRow.toString(), null);
        memberList.add(declLeftRow);

        server = rightRel.mdrClassExtent.directory.server;

        varRepository = implementor.newVariable();
        FieldDeclaration declRepository =
            new FieldDeclaration(new ModifierList(ModifierList.PRIVATE),
                OJUtil.typeNameForClass(MDRepository.class),
                varRepository.toString(), null);
        memberList.add(declRepository);
    }

    private void generateGetNextRightIterator()
    {
        stmtList = new StatementList();
        MemberDeclaration getNextRightIteratorMethodDecl =
            new MethodDeclaration(new ModifierList(ModifierList.PROTECTED),
                OJUtil.typeNameForClass(Object.class), "getNextRightIterator",
                new ParameterList(), null, stmtList);
        memberList.add(getNextRightIteratorMethodDecl);

        stmtList.add(
            new ExpressionStatement(
                new AssignmentExpression(
                    varLeftRow,
                    AssignmentExpression.EQUALS,
                    new CastExpression(
                        TypeName.forOJClass(leftRowClass),
                        new FieldAccess("leftObj")))));

        Expression iterExpr;
        if (joinRel.getRightReference() == null) {
            iterExpr = generateManyToOneLookup();
        } else {
            iterExpr = generateOneToManyLookup();
        }

        stmtList.add(new ReturnStatement(iterExpr));
    }

    private void generateCalcJoinRow()
    {
        stmtList = new StatementList();
        MemberDeclaration calcJoinRowMethodDecl =
            new MethodDeclaration(new ModifierList(ModifierList.PROTECTED),
                OJUtil.typeNameForClass(Object.class), "calcJoinRow",
                new ParameterList(), null, stmtList);
        memberList.add(calcJoinRowMethodDecl);

        if ((joinRel.getRightReference() == null)
                || !joinRel.getRightReference().getType().equals(
                    rightRel.mdrClassExtent.refClass.refMetaObject())) {
            // since the right-hand input is more specific than the
            // corresponding association end, we have to filter out any
            // unrelated types we might encounter
            Expression instanceofExpr;
            if (rightRel.useReflection) {
                varRightClassifier = implementor.newVariable();
                FieldDeclaration declRightClassifier =
                    new FieldDeclaration(new ModifierList(ModifierList.PRIVATE),
                        OJUtil.typeNameForClass(RefObject.class),
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
                        OJUtil.typeNameForClass(rightRel.rowClass));
            }
            stmtList.add(
                new IfStatement(
                    new UnaryExpression(UnaryExpression.NOT, instanceofExpr),
                    new StatementList(
                        new ReturnStatement(Literal.constantNull()))));
        }

        RexNode [] rightExps =
            rightRel.implementProjection(new FieldAccess("rightObj"));

        generateRowCalc(rightExps);
        stmtList.add(new ReturnStatement(varOutputRow));
    }

    private void generateOpen()
    {
        stmtList = new StatementList();
        MemberDeclaration openMethodDecl =
            new MethodDeclaration(new ModifierList(ModifierList.PROTECTED),
                TypeName.forOJClass(OJSystem.VOID), "open",
                new ParameterList(), null, stmtList);
        memberList.add(openMethodDecl);

        stmtList.add(
            new ExpressionStatement(
                new AssignmentExpression(
                    varRepository,
                    AssignmentExpression.EQUALS,
                    new CastExpression(
                        OJUtil.typeNameForClass(MDRepository.class),
                        server.generateRuntimeSupportCall(
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

        // for an outer join, construct the calcRightNullRow method
        if (joinRel.getJoinType() == JoinRelType.LEFT) {
            stmtList = new StatementList();
            MemberDeclaration calcRightNullRowMethodDecl =
                new MethodDeclaration(new ModifierList(ModifierList.PROTECTED),
                    OJUtil.typeNameForClass(Object.class),
                    "calcRightNullRow",
                    new ParameterList(),
                    null,
                    stmtList);
            memberList.add(calcRightNullRowMethodDecl);
            generateRowCalc(null);
            stmtList.add(new ReturnStatement(varOutputRow));
        }
    }

    private void generateRowCalc(RexNode [] rightExps)
    {
        RelDataTypeField [] fields = outputRowType.getFields();
        int nLeft = leftRowType.getFieldList().size();
        int n = fields.length;
        for (int i = 0; i < n; i++) {
            Expression lhs =
                new FieldAccess(varOutputRow,
                    Util.toJavaId(
                        fields[i].getName(),
                        i));
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
                                Util.toJavaId(
                                    leftFields[i].getName(),
                                    i)))));
            } else {
                RexNode rhs;
                if (rightExps == null) {
                    // generate a left outer join mismatch
                    rhs = implementor.getRexBuilder().constantNull();
                } else {
                    // generate a real join row
                    rhs = rightExps[i - nLeft];
                }
                final RexToOJTranslator translator =
                    implementor.newStmtTranslator(
                        joinRel,
                        stmtList,
                        memberList);
                translator.translateAssignment(
                    fields[i],
                    lhs,
                    rhs);
            }
        }
    }

    private Expression generateOneToManyLookup()
    {
        association =
            (Association) joinRel.getRightReference().getReferencedEnd()
                .getContainer();

        // TODO:  preserve the left type in the FarragoType system instead
        leftKeyClassifier =
            joinRel.getRightReference().getReferencedEnd().getType();
        leftKeyRefClass =
            (RefClass) rightRel.getRefObjectFromModelElement(leftKeyClassifier);
        leftKeyClass = JmiUtil.getClassForRefClass(leftKeyRefClass);

        boolean useAssocReflection = rightRel.useReflection;

        if (leftKeyClass == RefObject.class) {
            useAssocReflection = true;
        }

        Variable varLeftObj = implementor.newVariable();
        stmtList.add(
            new VariableDeclaration(
                OJUtil.typeNameForClass(leftKeyClass),
                varLeftObj.toString(),
                new CastExpression(
                    OJUtil.typeNameForClass(leftKeyClass),
                    new MethodCall(
                        varRepository,
                        "getByMofId",
                        new ExpressionList(
                            new MethodCall(
                                new FieldAccess(
                                    varLeftRow,
                                    Util.toJavaId(
                                        leftFields[joinRel.getLeftOrdinal()]
                                            .getName(),
                                        joinRel.getLeftOrdinal())),
                                "toString",
                                new ExpressionList()))))));

        Expression collectionExpr = null;
        if (!useAssocReflection) {
            String accessorName =
                JmiUtil.getAccessorName(
                    joinRel.getRightReference().getExposedEnd());
            try {
                // verify that the desired accessor method actually exists
                // using Java reflection, which will throw if it
                // doesn't exist
                leftKeyClass.getMethod(
                    accessorName,
                    new Class[0]);

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

        if (useAssocReflection) {
            varRefAssociation = implementor.newVariable();
            FieldDeclaration declRefAssociation =
                new FieldDeclaration(new ModifierList(ModifierList.PRIVATE),
                    OJUtil.typeNameForClass(RefAssociation.class),
                    varRefAssociation.toString(), null);
            memberList.add(declRefAssociation);

            // generate the JMI reflective query call
            collectionExpr =
                new MethodCall(
                    varRefAssociation,
                    "refQuery",
                    new ExpressionList(
                        Literal.makeLiteral(
                            joinRel.getRightReference().getReferencedEnd()
                                .getName()),
                        varLeftObj));
        }
        return new MethodCall(
            collectionExpr,
            "iterator",
            new ExpressionList());
    }

    private Expression generateManyToOneLookup()
    {
        Variable varLeftObj = implementor.newVariable();
        stmtList.add(
            new VariableDeclaration(
                OJUtil.typeNameForClass(String.class),
                varLeftObj.toString(),
                new MethodCall(
                    new FieldAccess(
                        varLeftRow,
                        Util.toJavaId(
                            leftFields[joinRel.getLeftOrdinal()].getName(),
                            joinRel.getLeftOrdinal())),
                    "toString",
                    new ExpressionList())));
        Expression lookupExpr =
            new CastExpression(OJSystem.OBJECT,
                new MethodCall(varRepository,
                    "getByMofId",
                    new ExpressionList(varLeftObj)));
        return new ConditionalExpression(
            new BinaryExpression(
                varLeftObj,
                BinaryExpression.EQUAL,
                Literal.constantNull()),
            new CastExpression(
                OJSystem.OBJECT,
                new Variable("EMPTY_ITERATOR")),
            lookupExpr);
    }
}


// End MedMdrJoinRelImplementor.java
