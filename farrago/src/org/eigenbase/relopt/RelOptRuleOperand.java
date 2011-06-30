/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package org.eigenbase.relopt;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.util.*;


/**
 * A <code>RelOptRuleOperand</code> determines whether a {@link
 * org.eigenbase.relopt.RelOptRule} can be applied to a particular expression.
 *
 * <p>For example, the rule to pull a filter up from the left side of a join
 * takes operands: <code>(Join (Filter) (Any))</code>.</p>
 *
 * <p>Note that <code>children</code> means different things if it is empty or
 * it is <code>null</code>: <code>(Join (Filter <b>()</b>) (Any))</code> means
 * that, to match the rule, <code>Filter</code> must have no operands.</p>
 */
public class RelOptRuleOperand
{
    //~ Enums ------------------------------------------------------------------

    /**
     * Dummy type, containing a single value, for parameters to overloaded forms
     * of the {@link org.eigenbase.relopt.RelOptRuleOperand} constructor
     * signifying operands that will be matched by relational expressions with
     * any number of children.
     */
    public enum Dummy
    {
        /**
         * Signifies that operand can have any number of children.
         */
        ANY
    }

    //~ Instance fields --------------------------------------------------------

    private RelOptRuleOperand parent;
    private RelOptRule rule;

    // REVIEW jvs 29-Aug-2004: some of these are Volcano-specific and should be
    // factored out
    public int [] solveOrder;
    public int ordinalInParent;
    public int ordinalInRule;
    private final Predicate predicate;
    private final Class<? extends RelNode> clazz;
    private final RelOptRuleOperand [] children;
    public final boolean matchAnyChildren;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an operand.
     *
     * <p>If <code>children</code> is null, the rule matches regardless of the
     * number of children.
     *
     * <p>If <code>matchAnyChild</code> is true, child operands can be matched
     * in any order. This is useful when matching a relational expression which
     * can have a variable number of children. For example, the rule to
     * eliminate empty children of a Union would have operands
     *
     * <blockquote>Operand(UnionRel, true, Operand(EmptyRel))</blockquote>
     *
     * and given the relational expressions
     *
     * <blockquote>UnionRel(FilterRel, EmptyRel, ProjectRel)</blockquote>
     *
     * would fire the rule with arguments
     *
     * <blockquote>{Union, Empty}</blockquote>
     *
     * It is up to the rule to deduce the other children, or indeed the position
     * of the matched child.</p>
     *
     * @param clazz Class of relational expression to match (must not be null)
     * @param predicate Predicate to apply to relational expression after
     *     matching its class, or null to match any relational expression
     * @param matchAnyChild Whether child operands can be matched in any order
     * @param children Child operands; or null, meaning match any number of
     * children
     */
    public RelOptRuleOperand(
        Class<? extends RelNode> clazz,
        Predicate predicate,
        boolean matchAnyChild,
        RelOptRuleOperand ... children)
    {
        assert (clazz != null);
        this.clazz = clazz;
        this.predicate = predicate;
        this.children = children;
        if (children != null) {
            for (int i = 0; i < this.children.length; i++) {
                this.children[i].parent = this;
            }
        }
        this.matchAnyChildren = matchAnyChild;
    }

    /**
     * Creates an operand that matches a predicate and any number of children.
     *
     * @param clazz Class of relational expression to match (must not be null)
     * @param predicate Predicate to apply to relational expression after
     *     matching its class, or null to match any relational expression
     * @param dummy Dummy argument to distinguish this constructor from other
     * overloaded forms
     */
    public RelOptRuleOperand(
        Class<? extends RelNode> clazz,
        Predicate predicate,
        Dummy dummy)
    {
        this(
            clazz,
            predicate,
            false,
            (RelOptRuleOperand []) null);
        Util.discard(dummy);
    }

    /**
     * Creates an operand that matches child operands in the order they appear.
     *
     * <p>If <code>children</code> is null, the rule matches regardless of the
     * number of children.
     *
     * @param clazz Class of relational expression to match (must not be null)
     * @param children Child operands; must not be null
     */
    public RelOptRuleOperand(
        Class<? extends RelNode> clazz,
        RelOptRuleOperand ... children)
    {
        this(clazz, null, false, children);
        assert children != null;
    }

    /**
     * Creates an operand that matches any number of children.
     *
     * @param clazz Class of relational expression to match (must not be null)
     * @param dummy Dummy argument to distinguish this constructor from other
     * overloaded forms
     */
    public RelOptRuleOperand(
        Class<? extends RelNode> clazz,
        Dummy dummy)
    {
        this(clazz, null, false, (RelOptRuleOperand []) null);
        Util.discard(dummy);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the parent operand.
     *
     * @return parent operand
     */
    public RelOptRuleOperand getParent()
    {
        return parent;
    }

    /**
     * Sets the parent operand.
     *
     * @param parent Parent operand
     */
    public void setParent(RelOptRuleOperand parent)
    {
        this.parent = parent;
    }

    /**
     * Returns the rule this operand belongs to.
     *
     * @return containing rule
     */
    public RelOptRule getRule()
    {
        return rule;
    }

    /**
     * Sets the rule this operand belongs to
     *
     * @param rule containing rule
     */
    public void setRule(RelOptRule rule)
    {
        this.rule = rule;
    }

    public int hashCode()
    {
        int h = clazz.hashCode();
        h = Util.hash(
            h,
            predicate.hashCode());
        h = Util.hashArray(h, children);
        return h;
    }

    public boolean equals(Object obj)
    {
        if (!(obj instanceof RelOptRuleOperand)) {
            return false;
        }
        RelOptRuleOperand that = (RelOptRuleOperand) obj;
        return (this.clazz == that.clazz)
            && Util.equal(this.predicate, that.predicate)
            && Arrays.equals(this.children, that.children);
    }

    /**
     * @return relational expression class matched by this operand
     */
    public Class<? extends RelNode> getMatchedClass()
    {
        return clazz;
    }

    /**
     * Returns the child operands.
     *
     * @return child operands
     */
    public RelOptRuleOperand [] getChildOperands()
    {
        return children;
    }

    /**
     * Returns whether a relational expression matches this operand. It must be
     * of the right class and calling convention.
     */
    public boolean matches(RelNode rel)
    {
        return clazz.isInstance(rel)
               && ((predicate == null)
                   || predicate.evaluate(rel));
    }

    /**
     * Creates a predicate that matches relational expressions that have a
     * given trait.
     *
     * @param trait Trait
     * @return Predicate that matches relexps with a given trait
     */
    public static Predicate predicate(final RelTrait trait)
    {
        return trait == null ? null : new TraitPredicate(trait);
    }

    /**
     * Returns a predicate that matches a relational expression that if its
     * class is precisely the given class. Instances of subclasses do not pass.
     *
     * @param clazz Class
     * @return Predicate that matches relexps of exactly the given class
     */
    public static Predicate preciseInstance(Class<? extends RelNode> clazz)
    {
        return new PreciseInstancePredicate(clazz);
    }

    /**
     * Returns a predicate that matches a relational expression that is not
     * a subclass of a given class.
     *
     * @param clazz Class
     * @return Predicate that matches relexps not a subclass of given class
     */
    public static Predicate notInstance(Class<? extends RelNode> clazz)
    {
        return new NotInstancePredicate(clazz);
    }

    /**
     * Returns a predicate that matches a relational expression only if it
     * has (or does not have) system fields.
     *
     * <p>Typically this predicate is used in rules that transform joins.
     * Rules are often only applicable if the join does not have system fields.
     * Usually rules wait until {@link org.eigenbase.relopt.RelOptRule#onMatch}
     * to apply side-conditions, but joins do not usually have system fields, so
     * it's efficient to apply it as early as possible. This avoids cluttering
     * the volcano planner's rule queue.
     *
     * @param requireSysFields If true, matches only joins that have one or
     * more system fields; if false, matches only joins that have no system
     * fields
     *
     * @return Predicate that matches relexps if they have system fields
     */
    public static Predicate hasSystemFields(boolean requireSysFields)
    {
        return requireSysFields
               ? HasSystemFieldsPredicate.TRUE
               : HasSystemFieldsPredicate.FALSE;
    }

    /**
     * Returns a predicate that evaluates to true only if all sub-predicates
     * evaluate to true.
     *
     * @param predicates List of predicates
     * @return Predicate that computes logical AND of child predicates
     */
    public static Predicate and(
        Predicate... predicates)
    {
        return new AndPredicate(predicates.clone());
    }

    /**
     * Predicate on a relational expression.
     */
    public interface Predicate {
        /**
         * Evaluates the predicate.
         *
         * @param rel Relational expression
         * @return Result of evaluating the predicate
         */
        boolean evaluate(RelNode rel);
    }

    /**
     * Helper for {@link RelOptRuleOperand#predicate(RelTrait)}.
     */
    private static class TraitPredicate implements Predicate
    {
        private final RelTrait trait;

        public TraitPredicate(RelTrait trait)
        {
            assert trait != null;
            this.trait = trait;
        }

        public boolean evaluate(RelNode rel)
        {
            return rel.getTraits().contains(trait);
        }

        public int hashCode()
        {
            return trait.hashCode();
        }

        public boolean equals(Object obj)
        {
            return obj instanceof TraitPredicate
                && trait.equals(((TraitPredicate) obj).trait);
        }
    }

    /**
     * Helper for {@link RelOptRuleOperand#notInstance(Class)}.
     */
    private static class NotInstancePredicate implements Predicate
    {
        private final Class<? extends RelNode> clazz;

        NotInstancePredicate(Class<? extends RelNode> clazz)
        {
            this.clazz = clazz;
        }

        public boolean evaluate(RelNode rel)
        {
            return !clazz.isInstance(rel);
        }

        public int hashCode()
        {
            return clazz.hashCode();
        }

        public boolean equals(Object obj)
        {
            return obj instanceof NotInstancePredicate
                   && clazz.equals(((NotInstancePredicate) obj).clazz);
        }
    }

    /**
     * Helper for {@link RelOptRuleOperand#hasSystemFields(boolean)}.
     */
    private static class HasSystemFieldsPredicate
        implements Predicate
    {
        private final boolean requireSysFields;

        public static final Predicate TRUE = new HasSystemFieldsPredicate(true);

        public static final Predicate FALSE =
            new HasSystemFieldsPredicate(false);

        private HasSystemFieldsPredicate(boolean requireSysFields)
        {
            this.requireSysFields = requireSysFields;
        }

        public boolean evaluate(RelNode rel)
        {
            return rel.getSystemFieldList().isEmpty() != requireSysFields;
        }

        @Override
        public int hashCode()
        {
            return requireSysFields ? 1234 : 1235;
        }

        @Override
        public boolean equals(Object obj)
        {
            return obj instanceof HasSystemFieldsPredicate
                && requireSysFields
                   == ((HasSystemFieldsPredicate) obj).requireSysFields;
        }
    }

    /**
     * Helper for {@link RelOptRuleOperand#and(Predicate...)}.
     */
    private static class AndPredicate implements Predicate
    {
        private final Predicate[] predicates;

        AndPredicate(Predicate[] predicates)
        {
            this.predicates = predicates;
        }

        public boolean evaluate(RelNode rel)
        {
            for (Predicate predicate : predicates) {
                if (!predicate.evaluate(rel)) {
                    return false;
                }
            }
            return true;
        }

        public int hashCode()
        {
            return Arrays.hashCode(predicates);
        }

        public boolean equals(Object obj)
        {
            return (obj instanceof AndPredicate)
                && Arrays.equals(predicates, ((AndPredicate) obj).predicates);
        }
    }

    /**
     * Helper for {@link RelOptRuleOperand#and(Predicate...)}.
     */
    private static class PreciseInstancePredicate
        implements Predicate
    {
        private final Class<? extends RelNode> clazz;

        PreciseInstancePredicate(Class<? extends RelNode> clazz)
        {
            this.clazz = clazz;
        }

        public boolean evaluate(RelNode rel)
        {
            return rel.getClass() == clazz;
        }

        @Override
        public int hashCode()
        {
            return clazz.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            return obj instanceof PreciseInstancePredicate
                && clazz == ((PreciseInstancePredicate) obj).clazz;
        }
    }
}

// End RelOptRuleOperand.java
