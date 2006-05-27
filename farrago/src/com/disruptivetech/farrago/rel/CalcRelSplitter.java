/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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

package com.disruptivetech.farrago.rel;

import net.sf.farrago.trace.FarragoTrace;
import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.*;
import org.eigenbase.util.Util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * CalcRelSplitter operates on a {@link CalcRel} with multiple {@link RexCall}
 * sub-expressions that cannot all be implemented by a single concrete
 * {@link RelNode}.
 *
 * <p>For example, the Java and
 * Fennel calculator do not implement an identical set of operators.  The
 * CalcRel can be used to split a single CalcRel with mixed Java- and
 * Fennel-only operators into a tree of CalcRel object that can each be
 * individually implemented by either Java or Fennel.and splits it into several
 * CalcRel instances.
 *
 * <p>Currently the splitter is only capable of handling two "rel types".  That
 * is, it can deal with Java vs. Fennel CalcRels, but not Java vs. Fennel vs.
 * some other type of CalcRel.
 *
 * <p>See {@link FarragoAutoCalcRule} for an example of how this class is
 * used.
 */
public abstract class CalcRelSplitter
{
    //~ Static fields/initializers --------------------------------------------

    private static final Logger ruleTracer =
        FarragoTrace.getOptimizerRuleTracer();

    //~ Fields ----------------------------------------------------------------

    private final RexProgram program;
    private final RelDataTypeFactory typeFactory;

    private final RelType[] relTypes;
    private final RelOptCluster cluster;
    private final RelTraitSet traits;
    private final RelNode child;
    private final ImplementTester[] testers;

    /**
     * Constructs a CalcRelSplitter.
     *
     * @param calc CalcRel to split
     * @param relTypes Array of rel types, e.g. {Java, Fennel}.
     *                 Must be distinct.
     */
    CalcRelSplitter(CalcRel calc, RelType[] relTypes)
    {
        for (int i = 0; i < relTypes.length; i++) {
            assert relTypes[i] != null;
            for (int j = 0; j < i; j++) {
                assert relTypes[i] != relTypes[j] :
                    "Rel types must be distinct";
            }
        }
        this.program = calc.getProgram();
        this.cluster = calc.getCluster();
        this.traits = calc.getTraits();
        this.typeFactory = calc.getCluster().getTypeFactory();
        this.child = calc.getChild();
        this.relTypes = relTypes;
        this.testers = new ImplementTester[relTypes.length];
        for (int i = 0; i < relTypes.length; i++) {
            testers[i] = new ImplementTester(relTypes[i]);
        }
    }

    RelNode execute()
    {
        // Check that program is valid. In particular, this means that every
        // expression is trivial (either an atom, or a function applied to
        // references to atoms) and every expression depends only on
        // expressions to the left.
        assert program.isValid(true);
        final List<RexNode> exprList = program.getExprList();
        final RexNode[] exprs =
            (RexNode[]) exprList.toArray(new RexNode[exprList.size()]);
        assert !RexUtil.containComplexExprs(exprList);

        // Figure out what level each expression belongs to.
        int[] exprLevels = new int[exprs.length];

        // The reltype of a level is given by
        // relTypes[levelTypeOrdinals[level]].
        int[] levelTypeOrdinals = new int[exprs.length];

        int levelCount = chooseLevels(exprs, -1, exprLevels, levelTypeOrdinals);

        // For each expression, figure out which is the highest level where it
        // is used.
        int[] exprMaxUsingLevelOrdinals =
            new HighestUsageFinder(exprs, exprLevels).
            getMaxUsingLevelOrdinals();

        // If expressions are used as outputs, mark them as higher than that.
        final List<RexLocalRef> projectRefList = program.getProjectList();
        final RexLocalRef conditionRef = program.getCondition();
        for (RexLocalRef projectRef : projectRefList) {
            exprMaxUsingLevelOrdinals[projectRef.getIndex()] = levelCount;
        }
        if (conditionRef != null) {
            exprMaxUsingLevelOrdinals[conditionRef.getIndex()] = levelCount;
        }

        // Print out what we've got.
        if (ruleTracer.isLoggable(Level.FINER)) {
            traceLevelExpressions(
                exprs, exprLevels, levelTypeOrdinals, levelCount);
        }

        // Now build the calcs.
        RelNode rel = child;
        final int inputFieldCount = program.getInputRowType().getFieldCount();
        int[] inputExprOrdinals = identityArray(inputFieldCount);
        boolean doneCondition  = false;
        for (int level = 0; level < levelCount; level++) {
            final int[] projectExprOrdinals;
            final RelDataType outputRowType;
            if (level == levelCount - 1) {
                outputRowType = program.getOutputRowType();
                projectExprOrdinals = new int[projectRefList.size()];
                for (int i = 0; i < projectExprOrdinals.length; i++) {
                    projectExprOrdinals[i] = projectRefList.get(i).getIndex();
                }
            } else {
                outputRowType = null;
                // Project the expressions which are computed at this level or
                // before, and will be used at later levels.
                IntList projectExprOrdinalList = new IntList();
                for (int i = 0; i < exprs.length; i++) {
                    if (exprLevels[i] <= level &&
                        exprMaxUsingLevelOrdinals[i] > level) {
                        projectExprOrdinalList.add(i);
                    }
                }
                projectExprOrdinals = projectExprOrdinalList.toIntArray();
            }

            final RelType relType = relTypes[levelTypeOrdinals[level]];

            // Can we do the condition this level?
            int conditionExprOrdinal = - 1;
            if (conditionRef != null && !doneCondition) {
                conditionExprOrdinal = conditionRef.getIndex();
                if (exprLevels[conditionExprOrdinal] > level ||
                    !relType.supportsCondition()) {
                    // stand down -- we're not ready to do the condition yet
                    conditionExprOrdinal = - 1;
                } else {
                    doneCondition = true;
                }
            }

            RexProgram program1 = createProgramForLevel(
                level, rel.getRowType(), exprs, exprLevels, inputExprOrdinals,
                projectExprOrdinals, conditionExprOrdinal, outputRowType);
            rel = relType.makeRel(
                cluster, traits, program1.getOutputRowType(), rel, program1);
            // The outputs of this level will be the inputs to the next level.
            inputExprOrdinals = projectExprOrdinals;
        }

        Util.permAssert(
            doneCondition || conditionRef == null, "unhandled condition");
        return rel;
    }

    /**
     * Figures out which expressions to calculate at which level.
     *
     * @param exprs Array of expressions
     * @param conditionOrdinal Ordinal of the condition expression, or -1
     *   if no condition
     * @param exprLevels Level ordinal for each expression (output)
     * @param levelTypeOrdinals The type of each level (output)
     * @return
     */
    private int chooseLevels(
        final RexNode[] exprs,
        int conditionOrdinal,
        int[] exprLevels,
        int[] levelTypeOrdinals)
    {
        final int inputFieldCount = program.getInputRowType().getFieldCount();

        int levelCount = 0;
        final MaxInputFinder maxInputFinder = new MaxInputFinder(exprLevels);
        boolean[] relTypesPossibleForTopLevel = new boolean[relTypes.length];
        Arrays.fill(relTypesPossibleForTopLevel, true);
        for (int i = 0; i < exprs.length; i++) {
            RexNode expr = exprs[i];
            final boolean condition = i == conditionOrdinal;

            if (i < inputFieldCount) {
                assert expr instanceof RexInputRef;
                exprLevels[i] = -1;
                continue;
            }

            // Deduce the minimum level of the expression. An expression must
            // be at a level greater than or equal to all of its inputs.
            int level = maxInputFinder.maxInputFor(expr);

            // Try to implement this expression at this level.
            // If that is not possible, try to implement it at higher levels.
            levelLoop:
            for (;; ++level) {
                if (level >= levelCount) {
                    // This is a new level. We can use any reltype we like.
                    for (int relTypeOrdinal = 0;
                         relTypeOrdinal < relTypes.length; relTypeOrdinal++) {
                        if (!relTypesPossibleForTopLevel[relTypeOrdinal]) {
                            continue;
                        }
                        if (testers[relTypeOrdinal].canImplement(
                            expr, condition))
                        {
                            // Success. We have found a reltype where we can
                            // implement this expression.
                            exprLevels[i] = level;
                            levelTypeOrdinals[level] = relTypeOrdinal;
                            assert level == 0 ||
                                levelTypeOrdinals[level - 1] !=
                                levelTypeOrdinals[level] :
                                "successive levels of same type";

                            // Figure out which of the other reltypes are
                            // still possible for this level.
                            // Previous reltypes are not possible.
                            for (int j = 0; j < relTypeOrdinal; ++j) {
                                relTypesPossibleForTopLevel[j] = false;
                            }
                            // Successive reltypes may be possible.
                            for (int j = relTypeOrdinal + 1;
                                 j < relTypes.length; ++j) {
                                if (relTypesPossibleForTopLevel[j]) {
                                    relTypesPossibleForTopLevel[j] =
                                        testers[j].canImplement(
                                            expr, condition);
                                }
                            }
                            // Move to next level.
                            levelTypeOrdinals[levelCount] =
                                firstSet(relTypesPossibleForTopLevel);
                            ++levelCount;
                            Arrays.fill(relTypesPossibleForTopLevel, true);
                            break levelLoop;
                        }
                    }
                    // None of the reltypes still active for this level could
                    // implement expr. But maybe we could succeed with a new
                    // level, with all options open?
                    if (count(relTypesPossibleForTopLevel) < relTypes.length) {
                        // Move to next level.
                        levelTypeOrdinals[levelCount] =
                            firstSet(relTypesPossibleForTopLevel);
                        ++levelCount;
                        Arrays.fill(relTypesPossibleForTopLevel, true);
                        continue;
                    } else {
                        // Cannot implement for any reltype.
                        throw Util.newInternal("cannot implement " + expr);
                    }
                } else {
                    final int levelTypeOrdinal = levelTypeOrdinals[level];
                    final ImplementTester tester = testers[levelTypeOrdinal];
                    if (!tester.canImplement(expr, condition)) {
                        // Cannot implement this expression in this reltype;
                        // continue to next level.
                        continue;
                    }
                    exprLevels[i] = level;
                    break;
                }
            }
        }
        return levelCount;
    }

    private int[] identityArray(int length)
    {
        final int[] ints = new int[length];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = i;
        }
        return ints;
    }

    /**
     * Creates a program containing the expressions for a given level.
     *
     * <p>The expression list of the program will consist of all entries in
     * the expression list <code>allExprs[i]</code> for which the corresponding
     * level ordinal <code>exprLevels[i]</code> is equal to <code>level</code>.
     * Expressions are mapped according to <code>inputExprOrdinals</code>.
     *
     * @param level Level ordinal
     * @param inputRowType Input row type
     * @param allExprs Array of all expressions
     * @param exprLevels Array of the level ordinal of each expression
     * @param inputExprOrdinals Ordinals in the expression list of input
     *         expressions. Input expression <code>i</code> will be found at
     *         position <code>inputExprOrdinals[i]</code>.
     * @param projectExprOrdinals Ordinals of the expressions to be output
     *   this level.
     * @param conditionExprOrdinal Ordinal of the expression to form the
     *   condition for this level, or -1 if there is no condition.
     * @param outputRowType Output row type
     * @return Relational expression
     */
    private RexProgram createProgramForLevel(
        int level,
        RelDataType inputRowType,
        RexNode[] allExprs,
        int[] exprLevels,
        int[] inputExprOrdinals,
        int[] projectExprOrdinals,
        int conditionExprOrdinal,
        RelDataType outputRowType)
    {
        // Count how many expressions are going to be at this level.
        int exprCount = inputExprOrdinals.length;
        for (int i = 0; i < allExprs.length; i++) {
            if (exprLevels[i] == level) {
                ++exprCount;
            }
        }
        // Build a list of expressions to form the calc.
        RexNode[] exprs = new RexNode[exprCount];
        // exprOrdinals describes what position in allExprs a given expression
        // in inputExprOrdinals goes to
        int[] exprOrdinals = new int[exprCount];
        // exprInverseOrdinals describes where an expression in allExprs comes
        // from -- from an input, from a calculated expression, or -1 if not
        // available at this level.
        int[] exprInverseOrdinals = new int[allExprs.length];
        Arrays.fill(exprInverseOrdinals, -1);
        int j = 0;

        // First populate the inputs. They were computed at some previous level
        // and are used here.
        for (int i = 0; i < inputExprOrdinals.length; i++) {
            final int inputExprOrdinal = inputExprOrdinals[i];
            exprs[j] = new RexInputRef(i, allExprs[inputExprOrdinal].getType());
            exprOrdinals[j] = inputExprOrdinal;
            exprInverseOrdinals[inputExprOrdinal] = j;
            ++j;
        }
        // Next populate the computed expressions.
        final RexShuttle shuttle =
            new InputToCommonExprConverter(
                exprInverseOrdinals, exprLevels, level, inputExprOrdinals);
        for (int i = 0; i < allExprs.length; i++) {
            if (exprLevels[i] == level) {
                RexNode expr = allExprs[i];
                final RexNode translatedExpr = expr.accept(shuttle);
                exprs[j] = translatedExpr;
                exprOrdinals[j] = i;
                assert exprInverseOrdinals[i] == -1;
                exprInverseOrdinals[i] = j;
                ++j;
            }
        }
        assert j == exprCount;
        // Form the projection and condition list. Project and condition
        // ordinals are offsets into allExprs, so we need to map them into
        // exprs.
        final RexLocalRef[] projectRefs =
            new RexLocalRef[projectExprOrdinals.length];
        for (int i = 0; i < projectRefs.length; i++) {
            final int projectExprOrdinal = projectExprOrdinals[i];
            final int index = exprInverseOrdinals[projectExprOrdinal];
            assert index >= 0;
            projectRefs[i] =
                new RexLocalRef(index, allExprs[projectExprOrdinal].getType());
        }
        RexLocalRef conditionRef;
        if (conditionExprOrdinal >= 0) {
            final int index = exprInverseOrdinals[conditionExprOrdinal];
            conditionRef = new RexLocalRef(
                index, allExprs[conditionExprOrdinal].getType());
        } else {
            conditionRef = null;
        }
        if (outputRowType == null) {
            outputRowType = typeFactory.createStructType(
                new RelDataTypeFactory.FieldInfo()
                {
                    public int getFieldCount()
                    {
                        return projectRefs.length;
                    }

                    public String getFieldName(int index)
                    {
                        return "$" + index;
                    }

                    public RelDataType getFieldType(int index)
                    {
                        return projectRefs[index].getType();
                    }
                }
            );
        }
        return new RexProgram(
            inputRowType,
            exprs,
            projectRefs,
            conditionRef,
            outputRowType);
    }

    /**
     * Traces the given array of level expression lists at the
     * finer level.
     *
     * @param exprs Array expressions
     * @param exprLevels For each expression, the ordinal of its level
     * @param levelTypeOrdinals For each level, the ordinal of its reltype
     *   in the {@link #relTypes} array
     * @param levelCount The number of levels
     */
    private void traceLevelExpressions(
        RexNode[] exprs,
        int[] exprLevels,
        int[] levelTypeOrdinals,
        int levelCount)
    {
        StringWriter traceMsg = new StringWriter();
        PrintWriter traceWriter = new PrintWriter(traceMsg);
        traceWriter.println("FarragoAutoCalcRule result expressions for: ");
        traceWriter.println(program.toString());

        for (int level = 0; level < levelCount; level++) {
            traceWriter.println("Rel Level " + level +
                ", type " + relTypes[levelTypeOrdinals[level]]);

            for (int i = 0; i < exprs.length; i++) {
                RexNode expr = exprs[i];
                assert exprLevels[i] >= -1 && exprLevels[i] < levelCount :
                    "expression's level is out of range";
                if (exprLevels[i] == level) {
                    traceWriter.println("\t" + i + ": " + expr);
                }
            }
            traceWriter.println();
        }
        String msg = traceMsg.toString();
        ruleTracer.finer(msg);
    }


    /**
     * Returns the number of bits set in an array.
     */
    private static int count(boolean[] booleans)
    {
        int count = 0;
        for (int i = 0; i < booleans.length; i++) {
            if (booleans[i]) {
                ++count;
            }
        }
        return count;
    }

    /**
     * Returns the index of the first set bit in an array.
     */
    private static int firstSet(boolean[] booleans)
    {
        for (int i = 0; i < booleans.length; i++) {
            if (booleans[i]) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Searches for a value in a map, and returns the position where it was
     * found, or -1.
     *
     * @param index
     * @param map
     * @return
     */
    private static int indexOf(int index, int[] map)
    {
        for (int i = 0; i < map.length; i++) {
            if (index == map[i]) {
                return i;
            }
        }
        return -1;
    }

    //~ Inner Classes ---------------------------------------------------------

    public abstract static class RelType
    {
        private final String name;

        public RelType(String name)
        {
            this.name = name;
        }

        public String toString()
        {
            return name;
        }

        protected abstract boolean canImplement(RexFieldAccess field);

        protected abstract boolean canImplement(RexDynamicParam param);

        protected abstract boolean canImplement(RexLiteral literal);

        protected abstract boolean canImplement(RexCall call);

        protected boolean supportsCondition()
        {
            return true;
        }

        protected RelNode makeRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelDataType rowType,
            RelNode child,
            RexProgram program)
        {
            return new CalcRel(
                cluster,
                traits,
                child,
                rowType,
                program,
                RelCollation.emptyList);
        }
    }


    /**
     * Visitor which returns whether an expression can be implemented in a
     * given type of relational expression.
     */
    private class ImplementTester extends RexVisitorImpl<Void>
    {
        private final RelType relType;

        public ImplementTester(RelType relType)
        {
            super(false);
            this.relType = relType;
        }

        public Void visitCall(RexCall call)
        {
            if (!relType.canImplement(call)) {
                throw CannotImplement.instance;
            }
            return null;
        }

        public Void visitDynamicParam(RexDynamicParam dynamicParam)
        {
            if (!relType.canImplement(dynamicParam)) {
                throw CannotImplement.instance;
            }
            return null;
        }

        public Void visitFieldAccess(RexFieldAccess fieldAccess)
        {
            if (!relType.canImplement(fieldAccess)) {
                throw CannotImplement.instance;
            }
            return null;
        }

        public Void visitLiteral(RexLiteral literal)
        {
            if (!relType.canImplement(literal)) {
                throw CannotImplement.instance;
            }
            return null;
        }

        public boolean canImplement(RexNode expr, boolean condition)
        {
            if (condition && !relType.supportsCondition()) {
                return false;
            }
            try {
                expr.accept(this);
                return true;
            } catch (CannotImplement e) {
                Util.swallow(e, null);
                return false;
            }
        }
    }

    /**
     * Control exception for {@link ImplementTester}.
     */
    private static class CannotImplement extends RuntimeException
    {
        static final CannotImplement instance = new CannotImplement();
    }

    /**
     * Shuttle which converts every reference to an input field in an
     * expression to a reference to a common sub-expression.
     */
    private static class InputToCommonExprConverter extends RexShuttle
    {
        private final int[] exprInverseOrdinals;
        private final int[] exprLevels;
        private final int level;
        private final int[] inputExprOrdinals;

        public InputToCommonExprConverter(
            int[] exprInverseOrdinals,
            int[] exprLevels,
            int level,
            int[] inputExprOrdinals)
        {
            this.exprInverseOrdinals = exprInverseOrdinals;
            this.exprLevels = exprLevels;
            this.level = level;
            this.inputExprOrdinals = inputExprOrdinals;
        }

        public RexNode visitInputRef(RexInputRef input)
        {
            final int index = exprInverseOrdinals[input.getIndex()];
            assert index >= 0;
            return new RexLocalRef(index, input.getType());
        }

        public RexNode visitLocalRef(RexLocalRef local)
        {
            // A reference to a local variable becomes a reference to an input
            // if the local was computed at a previous level.
            final int localIndex = local.getIndex();
            if (exprLevels[localIndex] < level) {
                int inputIndex = indexOf(localIndex, inputExprOrdinals);
                assert inputIndex >= 0;
                return new RexLocalRef(inputIndex, local.getType());
            } else {
                // It's a reference to what was a local expression at the
                // previous level, and was then projected.
                final int exprIndex = exprInverseOrdinals[localIndex];
                return new RexLocalRef(exprIndex, local.getType());
            }
        }
    }

    /**
     * Extension to {@link ArrayList} to help build an array of
     * <code>int</code> values.
     */
    private static class IntList extends ArrayList
    {
        public void add(int i)
        {
            add(new Integer(i));
        }
        public int[] toIntArray()
        {
            final int[] ints = new int[size()];
            for (int i = 0; i < ints.length; i++) {
                ints[i] = ((Integer) get(i)).intValue();
            }
            return ints;
        }
    }

    /**
     * Finds the highest level used by any of the inputs of a given expression.
     */
    private static class MaxInputFinder extends RexVisitorImpl<Void>
    {
        int level;
        private final int[] exprLevels;

        MaxInputFinder(int[] exprLevels)
        {
            super(true);
            this.exprLevels = exprLevels;
        }

        public Void visitLocalRef(RexLocalRef localRef)
        {
            int inputLevel = exprLevels[localRef.getIndex()];
            level = Math.max(level, inputLevel);
            return null;
        }

        /**
         * Returns the highest level of any of the inputs of an expression.
         */
        public int maxInputFor(RexNode expr)
        {
            level = 0;
            expr.accept(this);
            return level;
        }
    }

    /**
     * Builds an array of the highest level which contains an expression
     * which uses each expression as an input.
     */
    private static class HighestUsageFinder extends RexVisitorImpl<Void>
    {
        private final int[] maxUsingLevelOrdinals;
        private int currentLevel;

        public HighestUsageFinder(RexNode[] exprs, int[] exprLevels)
        {
            super(true);
            this.maxUsingLevelOrdinals = new int[exprs.length];
            Arrays.fill(maxUsingLevelOrdinals, -1);
            for (int i = 0; i < exprs.length; i++) {
                currentLevel = exprLevels[i];
                exprs[i].accept(this);
            }
        }

        public int[] getMaxUsingLevelOrdinals()
        {
            return maxUsingLevelOrdinals;
        }

        public Void visitLocalRef(RexLocalRef ref)
        {
            final int index = ref.getIndex();
            maxUsingLevelOrdinals[index] =
                Math.max(maxUsingLevelOrdinals[index], currentLevel);
            return null;
        }
    }
}

// End CalcRelSplitter.java
