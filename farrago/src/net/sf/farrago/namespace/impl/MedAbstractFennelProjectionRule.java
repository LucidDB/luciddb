/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package net.sf.farrago.namespace.impl;

import java.util.*;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;


/**
 * MedAbstractFennelProjectionRule is a base class for implementing projection
 * rules on different storage mechanisms
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public abstract class MedAbstractFennelProjectionRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new MedAbstractFennelProjectionRule object.
     *
     * @param operand root operand to pass to RelOptRule constructor
     */
    public MedAbstractFennelProjectionRule(RelOptRuleOperand operand)
    {
        super(operand);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    public abstract void onMatch(RelOptRuleCall call);

    /**
     * Creates projection list for scan. If the projection contains expressions,
     * then the input references from those expressions are extracted and that
     * list of references becomes the projection list.
     *
     * @param origScan row scan underneath the project
     * @param projRel ProjectRel that we will be creating the projection for
     * @param projectedColumns returns a list of the projected column ordinals,
     * if it is possible to project
     * @param preserveExprs special expressions that should be preserved in the
     * projection
     * @param defaultExpr expression to be used in the projection if no fields
     * or special columns are selected
     * @param newProjList returns a new projection RelNode corresponding to a
     * projection that now references a rowscan that is projecting the input
     * references that were extracted from the original projection expressions;
     * if the original expression didn't contain expressions, then this list is
     * returned empty
     *
     * @return true if columns in projection list from the scan need to be
     * renamed
     */
    public boolean createProjectionList(
        FennelRel origScan,
        ProjectRel projRel,
        List<Integer> projectedColumns,
        Set<SqlOperator> preserveExprs,
        RexNode defaultExpr,
        List<ProjectRel> newProjList)
    {
        // REVIEW:  what about AnonFields?
        int n = projRel.getChildExps().length;
        RelDataType rowType = origScan.getRowType();
        RelDataType projType = projRel.getRowType();
        RelDataTypeField [] projFields = projType.getFields();
        List<Integer> tempProjList = new ArrayList<Integer>();
        boolean needRename = false;
        for (int i = 0; i < n; ++i) {
            RexNode exp = projRel.getChildExps()[i];
            List<String> origFieldName = new ArrayList<String>();
            Integer projIndex = mapProjCol(exp, origFieldName, rowType);
            if (projIndex == null) {
                // there are expressions in the projection; we need to extract
                // all input references and any special expressions from the
                // projection
                PushProjector pushProject =
                    new PushProjector(projRel, null, origScan, preserveExprs);
                ProjectRel newProject = pushProject.convertProject(defaultExpr);
                if (newProject == null) {
                    // can't do any further projection
                    return false;
                }
                newProjList.add(newProject);

                // using the input references we just extracted, it should now
                // be possible to create a projection for the row scan
                needRename =
                    createProjectionList(
                        origScan,
                        (ProjectRel) newProject.getChild(),
                        projectedColumns,
                        preserveExprs,
                        defaultExpr,
                        newProjList);
                assert (projectedColumns.size() > 0);
                return needRename;
            }
            String projFieldName = projFields[i].getName();
            if (!projFieldName.equals(origFieldName.get(0))) {
                needRename = true;
            }
            tempProjList.add(projIndex);
        }

        // now that we've determined it is possible to project, add the
        // ordinals to the return list
        projectedColumns.addAll(tempProjList);
        return needRename;
    }

    /**
     * Maps a projection expression to its underlying field reference
     *
     * @param exp expression to be mapped
     * @param origFieldName returns field name corresponding to the field
     * reference
     * @param rowType row from which the field reference originated
     *
     * @return ordinal representing the projection element
     */
    protected Integer mapProjCol(
        RexNode exp,
        List<String> origFieldName,
        RelDataType rowType)
    {
        if (!(exp instanceof RexInputRef)) {
            return null;
        }
        return mapFieldRef(exp, origFieldName, rowType);
    }

    protected Integer mapFieldRef(
        RexNode exp,
        List<String> origFieldName,
        RelDataType rowType)
    {
        RexInputRef fieldAccess = (RexInputRef) exp;
        origFieldName.add(
            rowType.getFields()[fieldAccess.getIndex()].getName());
        return fieldAccess.getIndex();
    }

    /**
     * Creates new RelNodes replacing/removing the original project/row scan
     *
     * @param projectedScan new scan that is now projected
     * @param origProject original projection
     * @param needRename true if fields from the row scan need to be renamed
     * @param newProject projection that contains the new projection
     * expressions, in the case where the original projection cannot be removed
     * because it projects expressions
     *
     * @return new RelNode
     */
    public RelNode createNewRelNode(
        RelNode projectedScan,
        ProjectRel origProject,
        boolean needRename,
        ProjectRel newProject)
    {
        RelNode scanRel;
        if (needRename) {
            // Replace calling convention with FENNEL_EXEC_CONVENTION
            RelTraitSet traits = RelOptUtil.clone(origProject.getTraits());
            traits.setTrait(
                CallingConventionTraitDef.instance,
                FennelRel.FENNEL_EXEC_CONVENTION);
            scanRel =
                new FennelRenameRel(
                    origProject.getCluster(),
                    projectedScan,
                    RelOptUtil.getFieldNames(origProject.getRowType()),
                    traits);
        } else {
            scanRel = projectedScan;
        }

        if (newProject == null) {
            return scanRel;
        } else {
            // in the case where the projection had expressions, put the
            // new, modified projection on top of the projected row scan
            return (ProjectRel) CalcRel.createProject(
                scanRel,
                newProject.getProjectExps(),
                RelOptUtil.getFieldNames(newProject.getRowType()));
        }
    }
}

// End MedAbstractFennelProjectionRule.java
