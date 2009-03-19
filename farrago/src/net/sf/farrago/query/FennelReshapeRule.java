/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
//ompareProj
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

import java.nio.charset.*;

import java.util.*;

import net.sf.farrago.fem.fennel.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;


/**
 * FennelReshapeRule is a rule that converts a {@link CalcRel} into a {@link
 * FennelReshapeRel}, provided the {@link CalcRel} only references simple
 * projections and contains a simple condition, if it has a condition.
 *
 * <p>The projection is simple if it consists of only {@link RexInputRef}s or
 * CASTs of {@link RexInputRef}s where the cast effectively does not require any
 * actual data conversion or data validation.
 *
 * <p>The condition is simple if the expression is an AND of filters, where each
 * filter is of the form {@link RexInputRef} OP {@link RexLiteral}. Each {@link
 * RexInputRef} can only be referenced once, and OP is either =, &gt, &gt=, &lt,
 * or &lt=. However, the non-equality operators can only be referenced once.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FennelReshapeRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelReshapeRule object.
     */
    public FennelReshapeRule()
    {
        super(
            new RelOptRuleOperand(
                CalcRel.class,
                ANY));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        CalcRel calcRel = (CalcRel) call.rels[0];
        RexProgram program = calcRel.getProgram();

        // check the projection
        List<Integer> projOrdinals = new ArrayList<Integer>();
        RelDataType outputRowType = isProjectSimple(calcRel, projOrdinals);
        if (outputRowType == null) {
            return;
        }

        RexLocalRef condition = program.getCondition();
        CompOperatorEnum compOp = CompOperatorEnum.COMP_NOOP;
        Integer [] filterOrdinals = {};
        List<RexLiteral> filterLiterals = new ArrayList<RexLiteral>();

        // check the condition
        if (condition != null) {
            RexNode filterExprs = program.expandLocalRef(condition);
            List<Integer> filterList = new ArrayList<Integer>();

            List<CompOperatorEnum> op = new ArrayList<CompOperatorEnum>();
            if (!isConditionSimple(
                    calcRel,
                    filterExprs,
                    filterList,
                    filterLiterals,
                    op))
            {
                return;
            }

            compOp = op.get(0);
            filterOrdinals = filterList.toArray(new Integer[filterList.size()]);
        }

        RelNode fennelInput =
            mergeTraitsAndConvert(
                calcRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                calcRel.getChild());
        if (fennelInput == null) {
            return;
        }

        Integer [] projection =
            projOrdinals.toArray(new Integer[projOrdinals.size()]);
        FennelReshapeRel reshapeRel =
            new FennelReshapeRel(
                calcRel.getCluster(),
                fennelInput,
                projection,
                outputRowType,
                compOp,
                filterOrdinals,
                filterLiterals,
                new FennelRelParamId[] {},
                new Integer[] {},
                null);

        call.transformTo(reshapeRel);
    }

    /**
     * Determines if a projection is simple.
     *
     * @param calcRel CalcRel containing the projection
     * @param projOrdinals if the projection is simple, returns the ordinals of
     * the projection inputs
     *
     * @return rowtype corresponding to the projection, provided it is simple;
     * otherwise null is returned
     */
    private RelDataType isProjectSimple(
        CalcRel calcRel,
        List<Integer> projOrdinals)
    {
        // Loop through projection expressions.  If we find a non-simple
        // projection expression, simply return.
        RexProgram program = calcRel.getProgram();
        List<RexLocalRef> projList = program.getProjectList();
        int nProjExprs = projList.size();
        RelDataType [] types = new RelDataType[nProjExprs];
        String [] fieldNames = new String[nProjExprs];
        RelDataTypeField [] projFields = calcRel.getRowType().getFields();

        for (int i = 0; i < nProjExprs; i++) {
            RexNode projExpr = program.expandLocalRef(projList.get(i));
            if (projExpr instanceof RexInputRef) {
                projOrdinals.add(((RexInputRef) projExpr).getIndex());
                types[i] = projExpr.getType();
                fieldNames[i] = projFields[i].getName();
                continue;
            } else if (!(projExpr instanceof RexCall)) {
                return null;
            }

            RexCall rexCall = (RexCall) projExpr;
            if (rexCall.getOperator() != SqlStdOperatorTable.castFunc) {
                return null;
            }
            RexNode castOperand = rexCall.getOperands()[0];
            if (!(castOperand instanceof RexInputRef)) {
                return null;
            }
            RelDataType castType = projExpr.getType();
            RelDataType origType = castOperand.getType();
            if (isCastSimple(origType, castType)) {
                projOrdinals.add(((RexInputRef) castOperand).getIndex());
                types[i] = castType;
                fieldNames[i] = projFields[i].getName();
            } else {
                return null;
            }
        }

        // return the rowtype corresponding to the output of the projection
        return calcRel.getCluster().getTypeFactory().createStructType(
            types,
            fieldNames);
    }

    /**
     * Returns true if a type is a simple cast of another type. It is if the
     * cast type is nullable and the cast is one of the following:
     * <li>x TO x
     * <li>char(n) TO varchar(m)
     * <li>varchar(n) TO varchar(m)
     * <li>x not null TO x nullable
     *
     * @param origType original type passed into the cast operand
     * @param castType type the operand will be casted to
     *
     * @return true if the cast is simple
     */
    private boolean isCastSimple(RelDataType origType, RelDataType castType)
    {
        SqlTypeName origTypeName = origType.getSqlTypeName();
        SqlTypeName castTypeName = castType.getSqlTypeName();

        if (!(castType.isNullable())) {
            return false;
        }

        Charset origCharset = origType.getCharset();
        Charset castCharset = castType.getCharset();
        if ((origCharset != null) || (castCharset != null)) {
            if ((origCharset == null) || (castCharset == null)) {
                return false;
            }
            if (!origCharset.equals(castCharset)) {
                return false;
            }
        }

        return ((origType == castType)
            || ((origTypeName == SqlTypeName.CHAR)
                && (castTypeName == SqlTypeName.VARCHAR))
            ||

            ((origTypeName == SqlTypeName.VARCHAR)
                && (castTypeName == SqlTypeName.VARCHAR))
            ||

            ((origTypeName == castTypeName)
                && (origType.getPrecision() == castType.getPrecision())
                && ((origTypeName != SqlTypeName.DECIMAL)
                    || (origType.getScale() == castType.getScale()))
                && (!origType.isNullable() && castType.isNullable())));
    }

    /**
     * Determines if a filter condition is a simple one and returns the
     * parameters corresponding to the simple filters.
     *
     * @param calcRel original CalcRel
     * @param filterExprs filter expression being analyzed
     * @param filterList returns the list of filter ordinals in the simple
     * expression
     * @param literals returns the list of literals to be used in the simple
     * comparisons
     * @param op returns the operator to be used in the simple comparison
     *
     * @return true if the filter condition is simple
     */
    private boolean isConditionSimple(
        CalcRel calcRel,
        RexNode filterExprs,
        List<Integer> filterList,
        List<RexLiteral> literals,
        List<CompOperatorEnum> op)
    {
        SargFactory sargFactory =
            new SargFactory(calcRel.getCluster().getRexBuilder());
        SargRexAnalyzer rexAnalyzer = sargFactory.newRexAnalyzer(true);
        List<SargBinding> sargBindingList = rexAnalyzer.analyzeAll(filterExprs);

        // Currently, it's all or nothing.  So, if there are filters rejected
        // by the analyzer, we can't process a subset using the reshape
        // exec stream
        if (rexAnalyzer.getNonSargFilterRexNode() != null) {
            return false;
        }

        List<RexInputRef> filterCols = new ArrayList<RexInputRef>();
        List<RexNode> filterOperands = new ArrayList<RexNode>();
        if (FennelRelUtil.extractSimplePredicates(
                sargBindingList,
                filterCols,
                filterOperands,
                op))
        {
            for (RexInputRef filterCol : filterCols) {
                filterList.add(filterCol.getIndex());
            }
            for (RexNode operand : filterOperands) {
                literals.add((RexLiteral) operand);
            }
            return true;
        } else {
            return false;
        }
    }
}

// End FennelReshapeRule.java
