/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;


/**
 * PushProjector is a utility class used to perform operations used in push
 * projection rules.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PushProjector
{

    //~ Constructors -----------------------------------------------------------

    public PushProjector()
    {
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Decomposes a projection to the input references referenced by a
     * projection and a filter, either of which is optional. Creates a
     * projection containing all input references as well as preserving any
     * special expressions. Converts the original projection and/or filter to
     * reference the new projection. Then, finally puts on top a final
     * projection corresponding to the original projection.
     *
     * @param origProj the original projection
     * @param origFilter the optional filter
     * @param rel the child of the original projection
     * @param preserveExprs list of expressions that should be preserved in the
     * projection
     * @param defaultExpr expression to be used in the projection if no fields
     * or special columns are selected
     *
     * @return the converted projection if it makes sense to push elements of
     * the projection; otherwise returns null
     */
    public ProjectRel convertProject(
        ProjectRel origProj,
        RexNode origFilter,
        RelNode rel,
        Set<SqlOperator> preserveExprs,
        RexNode defaultExpr)
    {
        RelDataTypeField [] scanFields = rel.getRowType().getFields();
        int nScanFields = scanFields.length;

        RexNode [] origProjExprs = {};
        if (origProj != null) {
            origProjExprs = origProj.getChildExps();
        }

        // locate all fields referenced in the projection and filter
        BitSet projRefs = new BitSet(nScanFields);
        BitSet leftFields = new BitSet(nScanFields);
        RelOptUtil.setRexInputBitmap(leftFields, 0, nScanFields);
        List<RexNode> preserveLeft = new ArrayList<RexNode>();
        locateAllRefs(
            origProjExprs,
            origFilter,
            projRefs,
            leftFields,
            null,
            preserveExprs,
            preserveLeft,
            null);

        // if all columns are being selected (either explicitly in the
        // projection) or via a "select *", then there needs to be some
        // special expressions to preserve in the projection; otherwise,
        // there's no point in proceeding any further
        if (origProj == null) {
            if (preserveLeft.size() == 0) {
                return null;
            }

            // even though there is no projection, this is the same as
            // selecting all fields
            RelOptUtil.setRexInputBitmap(projRefs, 0, nScanFields);
        } else if ((projRefs.cardinality() == nScanFields)
            && (preserveLeft.size() == 0)) {
            return null;
        }

        // if nothing is being selected from the underlying rel, just
        // project the default expression passed in as a parameter or the
        // first column if there is no default expression
        if ((projRefs.cardinality() == 0) && (preserveLeft.size() == 0)) {
            if (defaultExpr != null) {
                preserveLeft.add(defaultExpr);
            } else if (nScanFields == 1) {
                return null;
            } else {
                projRefs.set(0);
            }
        }

        // create a new projection referencing all fields referenced in
        // either the project or the filter
        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        int newProjLength = projRefs.cardinality();
        RelNode newProject =
            createProjectRefsAndExprs(
                rexBuilder,
                projRefs,
                scanFields,
                null,
                0,
                newProjLength,
                preserveLeft,
                rel);

        int [] adjustments =
            getAdjustments(
                scanFields,
                projRefs,
                nScanFields,
                0);

        // if a filter was passed in, convert it to reference the projected
        // columns, placing it on top of the project just created
        RelNode projChild;
        if (origFilter != null) {
            RexNode newFilter =
                convertRefsAndExprs(
                    rexBuilder,
                    origFilter,
                    scanFields,
                    adjustments,
                    preserveLeft,
                    projRefs.cardinality(),
                    null,
                    0,
                    newProject.getRowType().getFields());
            projChild = CalcRel.createFilter(newProject, newFilter);
        } else {
            projChild = newProject;
        }

        // put the original project on top of the filter/project, converting
        // it to reference the modified projection list; otherwise, create
        // a projection that essentially selects all fields
        ProjectRel topProject =
            createNewProject(
                origProj,
                scanFields,
                adjustments,
                preserveLeft,
                projRefs.cardinality(),
                null,
                0,
                rexBuilder,
                projChild);

        return topProject;
    }

    /**
     * Sets a bitmap with all references found in an array of projection
     * expressions and a filter. References within a expressions that should be
     * preserved in the projection are not projected. Instead these expressions
     * are returned in one of two lists, depending on whether the expression
     * should be pushed to the left or right hand side of the parent RelNode.
     *
     * @param projExprs the array of projection expressions
     * @param filter the filter
     * @param projRefs the bitmap to be set
     * @param leftFields bitmap representing the fields in the left hand side of
     * the parent RelNode
     * @param rightFields bitmap representing the fields in the right hand side
     * of the aprent RelNode
     * @param preserveExprs expressions that should be preserved in the
     * projection
     * @param preserveLeft returns list of expressions corresponding to those
     * that should be pushed to the left hand side of the parent RelNode
     * @param preserveRight returns list of expressions corresponding to those
     * that should be pushed to the right hand side of the parent RelNode
     */
    public void locateAllRefs(
        RexNode [] projExprs,
        RexNode filter,
        BitSet projRefs,
        BitSet leftFields,
        BitSet rightFields,
        Set<SqlOperator> preserveExprs,
        List<RexNode> preserveLeft,
        List<RexNode> preserveRight)
    {
        new InputSpecialOpFinder(
            projRefs,
            leftFields,
            rightFields,
            preserveExprs,
            preserveLeft,
            preserveRight).apply(projExprs, filter);
    }

    /**
     * Creates a projection based on the inputs specified in a bitmap and the
     * expressions that need to be preserved. The expressions are appended after
     * the input references.
     *
     * @param rexBuilder rex builder
     * @param projRefs bitmap containing input references that will be projected
     * @param relFields the fields that the projection will be referencing
     * @param joinFields the fields from the parent RelNode
     * @param offset first input in the bitmap that this projection can possibly
     * reference
     * @param nInputRefs number of input references in the projection to be
     * built
     * @param projExprs expressions to be preserved and therefore need to be
     * added to the new projection list
     * @param projChild child that the projection will be created on top of
     *
     * @return created projection
     */
    public ProjectRel createProjectRefsAndExprs(
        RexBuilder rexBuilder,
        BitSet projRefs,
        RelDataTypeField [] relFields,
        RelDataTypeField [] joinFields,
        int offset,
        int nInputRefs,
        List<RexNode> projExprs,
        RelNode projChild)
    {
        // add on the input references
        int refIdx = offset - 1;
        int projLength = nInputRefs + projExprs.size();
        RexNode [] newProjExprs = new RexNode[projLength];
        String [] fieldNames = new String[projLength];
        int i;
        for (i = 0; i < nInputRefs; i++) {
            refIdx = projRefs.nextSetBit(refIdx + 1);
            assert (refIdx >= 0);
            newProjExprs[i] =
                rexBuilder.makeInputRef(
                    relFields[refIdx - offset].getType(),
                    refIdx - offset);
            fieldNames[i] = relFields[refIdx - offset].getName();
        }

        // add on the expressions that need to be preserved, converting the
        // arguments to reference the projected columns (if necessary)
        int [] adjustments = {};
        if ((projExprs.size() > 0) && (offset > 0)) {
            adjustments = new int[joinFields.length];
            for (int idx = offset; idx < joinFields.length; idx++) {
                adjustments[idx] = -offset;
            }
        }
        for (RexNode projExpr : projExprs) {
            RexNode newExpr;
            if (offset > 0) {
                newExpr =
                    projExpr.accept(
                        new RelOptUtil.RexInputConverter(
                            rexBuilder,
                            joinFields,
                            relFields,
                            adjustments));
            } else {
                newExpr = projExpr;
            }
            newProjExprs[i] = newExpr;
            RexCall call = (RexCall) projExpr;
            fieldNames[i] = call.getOperator().getName();
            i++;
        }

        return
            (ProjectRel) CalcRel.createProject(
                projChild,
                newProjExprs,
                fieldNames);
    }

    /**
     * Determines how much each input reference needs to be adjusted as a result
     * of projection
     *
     * @param relFields the original input reference fields
     * @param projRefs bitmap containing the projected fields
     * @param nFieldsLeft number of fields on the left hand side of the parent
     * RelNode
     * @param rightOffset additional amount the fields referencing the right
     * hand side of the parent RelNode need to be adjusted by
     *
     * @return array indicating how much each input needs to be adjusted by
     */
    public int [] getAdjustments(
        RelDataTypeField [] relFields,
        BitSet projRefs,
        int nFieldsLeft,
        int rightOffset)
    {
        int [] adjustments = new int[relFields.length];
        int newIdx = 0;
        for (int pos = projRefs.nextSetBit(0); pos >= 0;
            pos = projRefs.nextSetBit(pos + 1)) {
            adjustments[pos] = -(pos - newIdx);
            if (pos >= nFieldsLeft) {
                adjustments[pos] += rightOffset;
            }
            newIdx++;
        }
        return adjustments;
    }

    /**
     * Clones an expression tree and walks through it, adjusting each
     * RexInputRef index by some amount, and converting expressions that need to
     * be preserved to field references.
     *
     * @param rexBuilder builder for creating new RexInputRefs
     * @param fields fields where the RexInputRefs originally originated from
     * @param rex the expression
     * @param adjustments the amount to adjust each field reference by
     * @param preserveLeft list of expressions to be converted to input refs,
     * corresponding to expressions that need to be pushed to the left
     * @param firstLeftRef index corresponding to the field reference that the
     * first expression on the left will be converted to
     * @param preserveRight list of expressions to be converted to input refs,
     * corresponding to expressions that need to be pushed to the right
     * @param firstRightRef index corresponding to the field reference that the
     * first expression on the right will be converted to
     * @param projChildFields fields of the child of the project
     *
     * @return modified expression tree
     */
    public RexNode convertRefsAndExprs(
        RexBuilder rexBuilder,
        RexNode rex,
        RelDataTypeField [] fields,
        int [] adjustments,
        List<RexNode> preserveLeft,
        int firstLeftRef,
        List<RexNode> preserveRight,
        int firstRightRef,
        RelDataTypeField [] projChildFields)
    {
        return
            rex.accept(
                new RefAndExprConverter(
                    rexBuilder,
                    fields,
                    projChildFields,
                    adjustments,
                    preserveLeft,
                    firstLeftRef,
                    preserveRight,
                    firstRightRef));
    }

    /**
     * Creates a new projection based on an original projection passed in,
     * adjusting all input refs based on an adjustment array passed in. If there
     * was no original projection, create a new one that selects every field
     * from the underlying rel
     *
     * @param origProj the original projection on which this new project is
     * based
     * @param relFields the underlying fields referenced by the original project
     * @param adjustments array indicating how much each input reference should
     * be adjusted by
     * @param preserveLeft list of expressions to be converted to input refs,
     * corresponding to expressions that need to be pushed to the left
     * @param firstLeftRef index corresponding to the field reference that the
     * first expression on the left will be converted to
     * @param preserveRight list of expressions to be converted to input refs,
     * corresponding to expressions that need to be pushed to the right
     * @param firstRightRef index corresponding to the field reference that the
     * first expression on the right will be converted to
     * @param rexBuilder rex builder
     * @param projChild child of the new project
     *
     * @return the created projection
     */
    public ProjectRel createNewProject(
        ProjectRel origProj,
        RelDataTypeField [] relFields,
        int [] adjustments,
        List<RexNode> preserveLeft,
        int firstLeftRef,
        List<RexNode> preserveRight,
        int firstRightRef,
        RexBuilder rexBuilder,
        RelNode projChild)
    {
        RexNode [] projExprs;
        String [] fieldNames;
        RexNode [] origProjExprs = null;
        int origProjLength;
        if (origProj == null) {
            origProjLength = relFields.length;
        } else {
            origProjExprs = origProj.getChildExps();
            origProjLength = origProjExprs.length;
        }
        projExprs = new RexNode[origProjLength];
        fieldNames = new String[origProjLength];

        if (origProj != null) {
            for (int i = 0; i < origProjLength; i++) {
                projExprs[i] =
                    convertRefsAndExprs(
                        rexBuilder,
                        origProjExprs[i],
                        relFields,
                        adjustments,
                        preserveLeft,
                        firstLeftRef,
                        preserveRight,
                        firstRightRef,
                        projChild.getRowType().getFields());
                fieldNames[i] = origProj.getRowType().getFields()[i].getName();
            }
        } else {
            for (int i = 0; i < origProjLength; i++) {
                projExprs[i] =
                    rexBuilder.makeInputRef(
                        relFields[i].getType(),
                        i);
                fieldNames[i] = relFields[i].getName();
            }
        }

        ProjectRel projRel =
            (ProjectRel) CalcRel.createProject(
                projChild,
                projExprs,
                fieldNames);

        return projRel;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Visitor which builds a bitmap of the inputs used by an expressions, as
     * well as locating expressions corresponding to special operators.
     */
    private class InputSpecialOpFinder
        extends RexVisitorImpl<Void>
    {
        private final BitSet rexRefs;
        private final BitSet leftFields;
        private final BitSet rightFields;
        private final Set<SqlOperator> preserveExprs;
        private final List<RexNode> preserveLeft;
        private final List<RexNode> preserveRight;

        public InputSpecialOpFinder(
            BitSet rexRefs,
            BitSet leftFields,
            BitSet rightFields,
            Set<SqlOperator> preserveExprs,
            List<RexNode> preserveLeft,
            List<RexNode> preserveRight)
        {
            super(true);
            this.rexRefs = rexRefs;
            this.leftFields = leftFields;
            this.rightFields = rightFields;
            this.preserveExprs = preserveExprs;
            this.preserveLeft = preserveLeft;
            this.preserveRight = preserveRight;
        }

        public Void visitCall(RexCall call)
        {
            if (preserveExprs.contains(call.getOperator())) {
                // if the arguments of the expression only reference the
                // left hand side, preserve it on the left; similarly, if
                // it only references expressions on the right
                int totalFields = leftFields.size();
                if (rightFields != null) {
                    totalFields += rightFields.size();
                }
                BitSet exprArgs = new BitSet(totalFields);
                call.accept(new RelOptUtil.InputFinder(exprArgs));
                if (exprArgs.cardinality() > 0) {
                    if (RelOptUtil.contains(leftFields, exprArgs)) {
                        addExpr(preserveLeft, call);
                        return null;
                    } else if (RelOptUtil.contains(rightFields, exprArgs)) {
                        assert (preserveRight != null);
                        addExpr(preserveRight, call);
                        return null;
                    }
                }
                // if the expression arguments reference both the left and
                // right, fall through and don't attempt to preserve the
                // expression, but instead locate references and special
                // ops in the call operands
            }
            super.visitCall(call);
            return null;
        }

        public Void visitInputRef(RexInputRef inputRef)
        {
            rexRefs.set(inputRef.getIndex());
            return null;
        }

        /**
         * Applies this visitor to an array of expressions and an optional
         * single expression.
         */
        public void apply(RexNode [] exprs, RexNode expr)
        {
            RexProgram.apply(this, exprs, expr);
        }

        /**
         * Adds an expression to a list if the same expression isn't already in
         * the list. Expressions are identical if their digests are the same.
         *
         * @param exprList current list of expressions
         * @param newExpr new expression to be added
         */
        private void addExpr(List<RexNode> exprList, RexNode newExpr)
        {
            String newExprString = newExpr.toString();
            for (RexNode expr : exprList) {
                if (newExprString.compareTo(expr.toString()) == 0) {
                    return;
                }
            }
            exprList.add(newExpr);
        }
    }

    /**
     * Walks an expression tree, replacing input refs with new values to reflect
     * projection and converting special expressions to field references.
     */
    private class RefAndExprConverter
        extends RelOptUtil.RexInputConverter
    {
        private final List<RexNode> preserveLeft;
        private final int firstLeftRef;
        private final List<RexNode> preserveRight;
        private final int firstRightRef;

        public RefAndExprConverter(
            RexBuilder rexBuilder,
            RelDataTypeField [] srcFields,
            RelDataTypeField [] destFields,
            int [] adjustments,
            List<RexNode> preserveLeft,
            int firstLeftRef,
            List<RexNode> preserveRight,
            int firstRightRef)
        {
            super(rexBuilder, srcFields, destFields, adjustments);
            this.preserveLeft = preserveLeft;
            this.firstLeftRef = firstLeftRef;
            this.preserveRight = preserveRight;
            this.firstRightRef = firstRightRef;
        }

        public RexNode visitCall(RexCall call)
        {
            // if the expression corresponds to one that needs to be preserved,
            // convert it to a field reference; otherwise, convert the entire
            // expression
            int match =
                findExprInLists(
                    call,
                    preserveLeft,
                    firstLeftRef,
                    preserveRight,
                    firstRightRef);
            if (match >= 0) {
                return
                    rexBuilder.makeInputRef(
                        destFields[match].getType(),
                        match);
            }
            return super.visitCall(call);
        }

        /**
         * Looks for a matching RexNode from among two lists of RexNodes and
         * returns the offset into the list corresponding to the match, adjusted
         * by an amount, depending on whether the match was from the first or
         * second list.
         *
         * @param rex RexNode that is being matched against
         * @param rexList1 first list of RexNodes
         * @param adjust1 adjustment if match occurred in first list
         * @param rexList2 second list of RexNodes
         * @param adjust2 adjustment if match occurred in the second list
         *
         * @return index in the list corresponding to the matching RexNode; -1
         * if no match
         */
        private int findExprInLists(
            RexNode rex,
            List<RexNode> rexList1,
            int adjust1,
            List<RexNode> rexList2,
            int adjust2)
        {
            int match = findExprInList(rex, rexList1);
            if (match >= 0) {
                return match + adjust1;
            }

            if (rexList2 != null) {
                match = findExprInList(rex, rexList2);
                if (match >= 0) {
                    return match + adjust2;
                }
            }

            return -1;
        }

        private int findExprInList(RexNode rex, List<RexNode> rexList)
        {
            int match = 0;
            for (RexNode rexElement : rexList) {
                if (rexElement.toString().compareTo(rex.toString()) == 0) {
                    return match;
                }
                match++;
            }
            return -1;
        }
    }
}

// End PushProjector.java
