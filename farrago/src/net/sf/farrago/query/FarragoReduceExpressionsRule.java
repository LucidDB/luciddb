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

import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.rex.*;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.relopt.*;
import org.eigenbase.util.*;

import java.util.*;
import java.util.regex.*;
import java.util.logging.*;
import java.sql.*;

import net.sf.farrago.trace.*;
import net.sf.farrago.session.*;

/**
 * FarragoReduceExpressionsRule applies various simplifying transformations
 * on RexNode trees.  Currently, the only transformation is constant
 * reduction, which evaluates constant subtrees, replacing them with
 * a corresponding RexLiteral.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoReduceExpressionsRule extends RelOptRule
{
    private static final Logger tracer = FarragoTrace.getOptimizerRuleTracer();

    public static final Pattern EXCLUSION_PATTERN =
        Pattern.compile("FarragoReduceExpressionsRule.*");
    
    /**
     * Creates a new FarragoReduceExpressionsRule object.
     *
     * @param relClass class of rels to which this rule should apply
     */
    public FarragoReduceExpressionsRule(Class relClass)
    {
        super(new RelOptRuleOperand(
                relClass,
                null));
        description =
            "FarragoReduceExpressionsRule:"
            + ReflectUtil.getUnqualifiedClassName(relClass);
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        RelNode rel = call.rels[0];
        RexNode [] exps = rel.getChildExps();
        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

        // Find reducible expressions.
        List<RexNode> reducibleExps = findReducibleExps(
            rel.getCluster().getTypeFactory(),
            exps);
        if (reducibleExps.isEmpty()) {
            return;
        }

        // Compute the values they reduce to.
        List<RexNode> reducedValues = new ArrayList<RexNode>();
        ReentrantValuesStmt reentrantStmt =
            new ReentrantValuesStmt(
                rexBuilder,
                reducibleExps,
                reducedValues);
        FarragoSession session = getSession(rel);
        reentrantStmt.execute(session);
        if (reentrantStmt.failed) {
            return;
        }

        // For ProjectRel, we have to be sure to preserve the result
        // types, so add casts as needed.  For FilterRel, this isn't
        // necessary, and the presence of casts could hinder other
        // rules such as sarg analysis, which require bare literals.
        boolean addCasts = (rel instanceof ProjectRel);

        RexNode [] newExps = new RexNode[exps.length];
        System.arraycopy(exps, 0, newExps, 0, exps.length);
        exps = newExps;
        RexReplacer replacer = new RexReplacer(
            rexBuilder,
            reducibleExps,
            reducedValues,
            (rel instanceof ProjectRel));
        for (int i = 0; i < exps.length; ++i) {
            exps[i] = replacer.apply(exps[i]);
        }

        RelNode newRel;
        if (rel instanceof FilterRel) {
            assert(exps.length == 1);
            FilterRel oldRel = (FilterRel) rel;
            newRel = CalcRel.createFilter(
                oldRel.getChild(),
                exps[0]);
        } else if (rel instanceof ProjectRel) {
            ProjectRel oldRel = (ProjectRel) rel;
            newRel = new ProjectRel(
                oldRel.getCluster(),
                oldRel.getChild(),
                exps,
                oldRel.getRowType(),
                ProjectRel.Flags.Boxed,
                RelCollation.emptyList);
        } else {
            throw Util.needToImplement(rel);
        }
        call.transformTo(newRel);
    }

    private FarragoSession getSession(RelNode rel)
    {
        FarragoSessionPlanner planner = (FarragoSessionPlanner)
            rel.getCluster().getPlanner();
        FarragoSessionPreparingStmt preparingStmt =
            planner.getPreparingStmt();
        return preparingStmt.getSession();
    }

    private List<RexNode> findReducibleExps(
        RelDataTypeFactory typeFactory,
        RexNode [] exps)
    {
        List<RexNode> result = new ArrayList<RexNode>();
        ConstantGardener gardener = new ConstantGardener(
            typeFactory,
            result);
        for (RexNode exp : exps) {
            gardener.analyze(exp);
        }
        return result;
    }

    /**
     * Replaces expressions with their reductions.  Note that
     * we only have to look for RexCall, since nothing else is
     * reducible in the first place.
     */
    private static class RexReplacer extends RexShuttle
    {
        private final RexBuilder rexBuilder;
        private final List<RexNode> reducibleExps;
        private final List<RexNode> reducedValues;
        private final boolean addCasts;
        
        RexReplacer(
            RexBuilder rexBuilder,
            List<RexNode> reducibleExps,
            List<RexNode> reducedValues,
            boolean addCasts)
        {
            this.rexBuilder = rexBuilder;
            this.reducibleExps = reducibleExps;
            this.reducedValues = reducedValues;
            this.addCasts = addCasts;
        }

        // override RexShuttle
        public RexNode visitCall(final RexCall call)
        {
            int i = reducibleExps.indexOf(call);
            if (i == -1) {
                return super.visitCall(call);
            }
            RexNode replacement = reducedValues.get(i);
            if (addCasts && (replacement.getType() != call.getType())) {
                // Handle change from nullable to NOT NULL by claiming
                // that the result is still nullable, even though
                // we know it isn't.
                replacement = rexBuilder.makeCast(
                    call.getType(),
                    replacement);
            }
            return replacement;
        }
    }

    /**
     * Evaluates constant expressions via a reentrant query
     * of the form "VALUES (exp1, exp2, exp3, ...)".
     */
    private static class ReentrantValuesStmt
        extends FarragoReentrantStmt
    {
        private final RexBuilder rexBuilder;
        private final List<RexNode> exprs;
        private final List<RexNode> results;

        boolean failed;

        ReentrantValuesStmt(
            RexBuilder rexBuilder,
            List<RexNode> exprs,
            List<RexNode> results)
        {
            this.rexBuilder = rexBuilder;
            this.exprs = exprs;
            this.results = results;
        }
        
        protected void executeImpl()
            throws Exception
        {
            RelNode oneRowRel = new OneRowRel(
                getPreparingStmt().getRelOptCluster());
            RelNode projectRel = CalcRel.createProject(
                oneRowRel,
                exprs,
                null);

            // NOTE jvs 26-May-2006: To avoid an infinite loop, we need to
            // make sure the reentrant planner does NOT have
            // FarragoReduceExpressionsRule enabled!
            FarragoPreparingStmt preparingStmt =
                (FarragoPreparingStmt) getPreparingStmt();
            FarragoSessionPlanner reentrantPlanner =
                preparingStmt.getPlanner();
            reentrantPlanner.setRuleDescExclusionFilter(
                EXCLUSION_PATTERN);
            
            getStmtContext().prepare(
                projectRel, SqlKind.Select, true, getPreparingStmt());
            getStmtContext().execute();
            ResultSet resultSet = getStmtContext().getResultSet();
            resultSet.next();
            for (int i = 0; i < exprs.size(); ++i) {
                // REVIEW jvs 28-May-2006: There might be a few cases where we
                // could preserve precision better by calling the type-specific
                // getter method instead of getString.
                String val = resultSet.getString(i + 1);
                RexNode expr = exprs.get(i);
                RexNode result;
                if (val == null) {
                    result = rexBuilder.constantNull();
                    result = rexBuilder.makeCast(
                        expr.getType(),
                        result);
                } else {
                    SqlTypeName typeName = expr.getType().getSqlTypeName();
                    // TODO jvs 26-May-2006:  See comment on RexLiteral
                    // constructor regarding SqlTypeFamily.
                    typeName = broadenType(typeName);
                    RelDataType literalType =
                        rexBuilder.getTypeFactory().createTypeWithNullability(
                            expr.getType(),
                            false);
                    try {
                        result = RexLiteral.fromJdbcString(
                            literalType,
                            typeName,
                            val);
                    } catch (NumberFormatException ex) {
                        // NOTE jvs 28-May-2006:  This catches infinity
                        // and NaN, which can't be round-tripped
                        // through String and don't have a SQL literal
                        // representation anyway.  For those rare cases,
                        // we just fail the whole rule.
                        result = null;
                        failed = true;
                    }
                }
                if (tracer.isLoggable(Level.FINE)) {
                    tracer.fine(
                        "reduced expression " + expr
                        + " to result " + result);
                }
                results.add(result);
            }
            assert(!resultSet.next());
            resultSet.close();
        }
    }

    // TODO jvs 26-May-2006:  Get rid of this.
    private static SqlTypeName broadenType(SqlTypeName typeName)
    {
        if (SqlTypeFamily.ApproximateNumeric.getTypeNames().contains(typeName))
        {
            return SqlTypeName.Double;
        }
        else if (SqlTypeFamily.ExactNumeric.getTypeNames().contains(typeName))
        {
            return SqlTypeName.Decimal;
        }
        else if (SqlTypeFamily.Character.getTypeNames().contains(typeName))
        {
            return SqlTypeName.Char;
        }
        else if (SqlTypeFamily.Binary.getTypeNames().contains(typeName))
        {
            return SqlTypeName.Binary;
        } else {
            return typeName;
        }
    }
    
    /**
     * Beware of the 3 Bees.
     */
    private static class ConstantGardener extends RexVisitorImpl<Void>
    {
        enum Constancy {
            NON_CONSTANT,
            REDUCIBLE_CONSTANT,
            IRREDUCIBLE_CONSTANT
        };

        private final RelDataTypeFactory typeFactory;
        
        private final List<Constancy> stack;

        private final List<RexNode> result;
        
        ConstantGardener(RelDataTypeFactory typeFactory, List<RexNode> result)
        {
            // go deep
            super(true);
            this.typeFactory = typeFactory;
            this.result = result;
            stack = new ArrayList<Constancy>();
        }

        public void analyze(RexNode exp)
        {
            assert(stack.isEmpty());
            
            exp.accept(this);

            // Deal with top of stack
            assert(stack.size() == 1);
            Constancy rootConstancy = stack.get(0);
            if (rootConstancy == Constancy.REDUCIBLE_CONSTANT) {
                // The entire subtree was constant, so add it to the result.
                addResult(exp);
            }
            stack.clear();
        }

        private Void pushVariable()
        {
            stack.add(Constancy.NON_CONSTANT);
            return null;
        }

        private void addResult(RexNode exp)
        {
            // Cast of literal can't be reduced, so skip those (otherwise we'd
            // go into an infinite loop as we add them back).
            if (exp.getKind() == RexKind.Cast) {
                RexCall cast = (RexCall) exp;
                RexNode operand = cast.getOperands()[0];
                if (operand instanceof RexLiteral) {
                    return;
                }
            }
            result.add(exp);
        }

        public Void visitInputRef(RexInputRef inputRef)
        {
            return pushVariable();
        }

        public Void visitLiteral(RexLiteral literal)
        {
            stack.add(Constancy.IRREDUCIBLE_CONSTANT);
            return null;
        }

        public Void visitOver(RexOver over)
        {
            // assume non-constant (running SUM(1) looks constant but isn't)
            analyzeCall(over, Constancy.NON_CONSTANT);
            return null;
        }

        public Void visitCorrelVariable(RexCorrelVariable correlVariable)
        {
            return pushVariable();
        }

        public Void visitCall(RexCall call)
        {
            // assume REDUCIBLE_CONSTANT until proven otherwise
            analyzeCall(call, Constancy.REDUCIBLE_CONSTANT);
            return null;
        }

        private void analyzeCall(RexCall call, Constancy callConstancy)
        {
            // visit operands, pushing their states onto stack
            super.visitCall(call);

            // look for NON_CONSTANT operands
            int nOperands = call.getOperands().length;
            List<Constancy> operandStack = stack.subList(
                stack.size() - nOperands,
                stack.size());
            for (Constancy operandConstancy : operandStack) {
                if (operandConstancy == Constancy.NON_CONSTANT) {
                    callConstancy = Constancy.NON_CONSTANT;
                }
            }

            // Even if all operands are constant, the call itself may
            // be non-deterministic.
            if (!call.getOperator().isDeterministic()) {
                callConstancy = Constancy.NON_CONSTANT;
            }

            if (callConstancy == Constancy.NON_CONSTANT) {
                // any REDUCIBLE_CONSTANT children are now known to be maximal
                // reducible subtrees, so they can be added to the result
                // list
                for (int iOperand = 0; iOperand < nOperands; ++iOperand) {
                    Constancy constancy = operandStack.get(iOperand);
                    if (constancy == Constancy.REDUCIBLE_CONSTANT) {
                        addResult(call.getOperands()[iOperand]);
                    }
                }
            }

            // pop operands off of the stack
            operandStack.clear();

            // push constancy result for this call onto stack
            stack.add(callConstancy);
        }

        public Void visitDynamicParam(RexDynamicParam dynamicParam)
        {
            return pushVariable();
        }

        public Void visitRangeRef(RexRangeRef rangeRef)
        {
            return pushVariable();
        }

        public Void visitFieldAccess(RexFieldAccess fieldAccess)
        {
            return pushVariable();
        }
    }
}

// End FarragoReduceExpressionsRule.java
