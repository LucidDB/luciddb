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
package com.disruptivetech.farrago.volcano;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.logging.Level;

import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.rex.OJRexImplementorTableImpl;
import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rel.convert.ConverterRel;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.*;
import org.eigenbase.util.*;

/**
 * VolcanoPlanner optimizes queries by transforming expressions selectively
 * according to a dynamic programming algorithm.
 */
public class VolcanoPlanner extends AbstractRelOptPlanner
{
    //~ Instance fields -------------------------------------------------------

    protected RelSubset root;

    /**
     * If true, the planner keeps applying rules as long as they continue to
     * reduce the cost.  If false, the planner terminates as soon as it has
     * found any implementation, no matter how expensive.  The default is
     * false due to unresolved bugs with various rules.
     */
    protected boolean ambitious;

    /**
     * List of all operands of all rules. Any operand can be an 'entry point'
     * to a rule call, when a relexp is registered which matches the.
     */
    private final List<RelOptRuleOperand> allOperands =
        new ArrayList<RelOptRuleOperand>();

    /** List of all sets. Used only for debugging. */
    final List<RelSet> allSets = new ArrayList<RelSet>();

    /**
     * Canonical map from {@link String digest} to the unique {@link
     * RelNode relational expression} with that digest.
     */
    private final Map<String, RelNode> mapDigestToRel =
        new HashMap<String, RelNode>();

    /**
     * Map each registered expression ({@link RelNode}) to its equivalence
     * set ({@link RelSubset}).
     *
     * <p>We use an {@link IdentityHashMap} to simplify the process of
     * merging {@link RelSet} objects. Most {@link RelNode} objects are
     * identified by their digest, which involves the set that their child
     * relational expressions belong to. If those children belong to the same
     * set, we have to be careful, otherwise it gets incestuous.</p>
     */
    private final IdentityHashMap<RelNode, RelSubset> mapRel2Subset =
        new IdentityHashMap<RelNode, RelSubset>();

    /** List of all schemas which have been registered. */
    private final Set<RelOptSchema> registeredSchemas =
        new HashSet<RelOptSchema>();

    /** Holds rule calls waiting to be fired. */
    final RuleQueue ruleQueue = new RuleQueue(this);

    /** Holds the currently registered RelTraitDefs. */
    private final Set<RelTraitDef> traitDefs = new HashSet<RelTraitDef>();

    /**
     * Set of all registered rules.
     */
    private final Set<RelOptRule> ruleSet = new HashSet<RelOptRule>();

    private int nextSetId = 0;

    /**
     * Incremented every time a relational expression is registered or two
     * sets are merged. Tells us whether anything is going on.
     */
    private int registerCount;

    /**
     * Listener for this planner, or null if none set.
     */
    RelOptListener listener;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a uninitialized <code>VolcanoPlanner</code>.  To fully
     * initialize it, the caller must register the desired set of relations,
     * rules, and calling conventions.
     */
    public VolcanoPlanner()
    {
    }

    //~ Methods ---------------------------------------------------------------

    // REVIEW: SWZ: 3/1/2005: No one calls this.  Remove?
    // todo: pre-compute
    public RelOptRuleOperand [] getConversionOperands(
        CallingConvention toConvention)
    {
        List<RelOptRuleOperand> list = new ArrayList<RelOptRuleOperand>();
        for (RelOptRuleOperand operand : allOperands) {
            if (operand.getRule().getOutConvention() == toConvention) {
                list.add(operand);
            }
        }
        return (RelOptRuleOperand [])
            list.toArray(new RelOptRuleOperand[list.size()]);
    }

    // implement RelOptPlanner
    public boolean isRegistered(RelNode rel)
    {
        return mapRel2Subset.get(rel) != null;
    }

    public void setRoot(RelNode rel)
    {
        this.root = registerImpl(rel, null);

        // Making a node the root changes its importance.
        this.ruleQueue.recompute(this.root);
    }

    public RelNode getRoot()
    {
        return root;
    }

    /**
     * Find an expression's equivalence set.  If the expression is not
     * registered, return null.
     *
     * @pre rel != null
     */
    public RelSet getSet(RelNode rel)
    {
        assert rel != null : "pre: rel != null";
        final RelSubset subset = getSubset(rel);
        if (subset != null) {
            assert subset.set != null;
            return subset.set;
        }
        return null;
    }

    public boolean addRelTraitDef(RelTraitDef relTraitDef)
    {
        return traitDefs.add(relTraitDef);
    }

    public boolean addRule(RelOptRule rule)
    {
        if (ruleSet.contains(rule)) {
            // Rule already exists.
            return false;
        }
        final boolean added = ruleSet.add(rule);
        assert added;

        mapRuleDescription(rule);

        // REVIEW jvs 3-Apr-2006:  This initialization is now in RelOptRule's
        // constructor, so it can be deleted from here.  But solve-order
        // remains Volcano-specific for now.

        // Each of this rule's operands is an 'entry point' for a rule call.
        Walker operandWalker = new Walker(rule.getOperand());
        int ordinalInRule = 0;
        ArrayList operandsOfRule = new ArrayList();
        while (operandWalker.hasMoreElements()) {
            RelOptRuleOperand operand =
                (RelOptRuleOperand) operandWalker.nextElement();
            operand.setRule(rule);
            operand.setParent((RelOptRuleOperand) operandWalker.getParent());
            operand.ordinalInParent = operandWalker.getOrdinal();
            operand.ordinalInRule = ordinalInRule++;
            operandsOfRule.add(operand);
            allOperands.add(operand);
        }

        // Convert this rule's operands from a list to an array.
        rule.operands =
            (RelOptRuleOperand []) operandsOfRule.toArray(RelOptRuleOperand.noOperands);

        // Build each operand's solve-order.  Start with itself, then its
        // parent, up to the root, then the remaining operands in prefix
        // order.
        for (int j = 0; j < rule.operands.length; j++) {
            RelOptRuleOperand operand = rule.operands[j];
            operand.solveOrder = new int[rule.operands.length];
            int m = 0;
            for (RelOptRuleOperand o = operand; o != null; o = o.getParent()) {
                operand.solveOrder[m++] = o.ordinalInRule;
            }
            for (int k = 0; k < rule.operands.length; k++) {
                boolean exists = false;
                for (int n = 0; n < m; n++) {
                    if (operand.solveOrder[n] == k) {
                        exists = true;
                    }
                }
                if (!exists) {
                    operand.solveOrder[m++] = k;
                }
            }

            // Assert: operand appears once in the sort-order.
            assert (m == rule.operands.length);
        }

        // If this is a converter rule, check if the registered RelTraitDefs
        // which notification of its addition.
        if (rule instanceof ConverterRule) {
            ConverterRule converterRule = (ConverterRule) rule;

            final RelTraitSet ruleTraits = converterRule.getInTraits();

            for (RelTraitDef traitDef : traitDefs) {
                if (ruleTraits.getTrait(traitDef) == null) {
                    // Rule does not operate on this RelTraitDef.
                    continue;
                }

                traitDef.registerConverterRule(this, converterRule);
            }
        }

        return true;
    }

    public boolean removeRule(RelOptRule rule)
    {
        if (!ruleSet.remove(rule)) {
            // Rule was not present.
            return false;
        }
        // Remove description.
        unmapRuleDescription(rule);
        // Remove operands.
        for (Iterator<RelOptRuleOperand> operandIter = allOperands.iterator();
             operandIter.hasNext();) {
            RelOptRuleOperand operand = operandIter.next();
            if (operand.getRule().equals(rule)) {
                operandIter.remove();
            }
        }
        // Remove trait mappings. (In particular, entries from conversion
        // graph.)
        if (rule instanceof ConverterRule) {
            ConverterRule converterRule = (ConverterRule) rule;

            final RelTraitSet ruleTraits = converterRule.getInTraits();

            for (RelTraitDef traitDef : traitDefs) {
                if (ruleTraits.getTrait(traitDef) == null) {
                    // Rule does not operate on this RelTraitDef.
                    continue;
                }

                traitDef.deregisterConverterRule(this, converterRule);
            }
        }
        return true;
    }

    public boolean canConvert(RelTraitSet fromTraits, RelTraitSet toTraits)
    {
        assert(fromTraits.size() >= toTraits.size());

        boolean canConvert = true;
        for (int i = 0; i < toTraits.size() && canConvert; i++) {
            RelTrait fromTrait = fromTraits.getTrait(i);
            RelTrait toTrait = toTraits.getTrait(i);

            assert fromTrait.getTraitDef() == toTrait.getTraitDef();
            assert traitDefs.contains(fromTrait.getTraitDef());
            assert traitDefs.contains(toTrait.getTraitDef());

            canConvert =
                fromTrait.getTraitDef().canConvert(this, fromTrait, toTrait);
        }

        return canConvert;
    }

    public RelNode changeTraits(final RelNode rel, RelTraitSet toTraits)
    {
        assert !rel.getTraits().equals(toTraits) :
            "pre: !rel.getTraits().equals(toTraits)";

        RelNode rel2 = ensureRegistered(rel);
        if (rel2.getTraits().equals(toTraits)) {
            return rel2;
        }

        RelNode rel3 = changeTraitsUsingConverters(rel2, toTraits, true);
        if (rel3 != null) {
            return rel3;
        }

        // REVIEW: SWZ: 3/5/2005: This is probably redundant.  The call
        // to changeTraitsUsingConverters should create a string of
        // AbstractConverter if none of the conversions is currently possible.
        RelNode converter = rel;
        for (int i = 0; i < toTraits.size(); i++) {
            RelTraitSet fromTraits = converter.getTraits();

            RelTrait fromTrait = fromTraits.getTrait(i);
            RelTrait toTrait = toTraits.getTrait(i);

            if (toTrait == null) {
               continue;
            }

            assert(fromTrait.getTraitDef() == toTrait.getTraitDef());

            if (fromTrait == toTrait) {
                // No need to convert, it's already correct.
                continue;
            }

            RelTraitSet stepTraits = RelOptUtil.clone(fromTraits);
            stepTraits.setTrait(toTrait.getTraitDef(), toTrait);

            converter =
                new AbstractConverter(
                    converter.getCluster(),
                    converter,
                    toTrait.getTraitDef(),
                    stepTraits);
        }

        // REVIEW: SWZ: 3/5/2005: Why is (was) this only done for abstract
        // converters?  Seems to me, the caller has to register the
        // conversion in the end anyway.
        return register(converter, rel);
    }

    public RelOptPlanner chooseDelegate()
    {
        return this;
    }

    public RelNode findBestExp()
    {
        RelOptCost targetCost = makeHugeCost();
        while (true) {
            if (root.bestCost.isLe(targetCost)) {
                if (ambitious) {
                    // Choose a more ambitious target cost, and try again
                    targetCost = root.bestCost.multiplyBy(0.5);
                } else {
                    break;
                }
            }
            if (!ruleQueue.hasNextMatch()) {
                break;
            }
            VolcanoRuleMatch match = ruleQueue.popMatch();
            match.onMatch();

            // The root may have been merged with another subset. Find the new
            // root subset.
            root = canonize(root);
        }
        if (tracer.isLoggable(Level.FINER)) {
            StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            dump(pw);
            pw.flush();
            tracer.finer(sw.toString());
        }
        return root.buildCheapestPlan(this);
    }

    // implement Planner
    public RelOptCost makeCost(
        double dRows,
        double dCpu,
        double dIo)
    {
        return new VolcanoCost(dRows, dCpu, dIo);
    }

    public RelOptCost makeHugeCost()
    {
        return VolcanoCost.HUGE;
    }

    public RelOptCost makeInfiniteCost()
    {
        return VolcanoCost.INFINITY;
    }

    public RelOptCost makeTinyCost()
    {
        return VolcanoCost.TINY;
    }

    public RelOptCost makeZeroCost()
    {
        return VolcanoCost.ZERO;
    }

    public RelNode register(
        RelNode rel,
        RelNode equivRel)
    {
        assert !isRegistered(rel) : "pre: isRegistered(rel)";
        final RelSet set;
        if (equivRel == null) {
            set = null;
        } else {
            assert RelOptUtil.equal(
                "rel rowtype", rel.getRowType(),
                "equivRel rowtype", equivRel.getRowType(), true);
            set = getSet(equivRel);
        }
        final RelSubset subset = registerImpl(rel, set);

        if (tracer.isLoggable(Level.FINE)) {
            validate();
        }

        return subset;
    }

    public RelNode ensureRegistered(RelNode rel)
    {
        final RelSubset subset = mapRel2Subset.get(rel);
        if (subset != null) {
            return subset;
        } else {
            return register(rel, null);
        }
    }

    /**
     * Checks internal consistency.
     */
    private void validate()
    {
        for (RelSet set : allSets) {
            if (set.equivalentSet != null) {
                throw new AssertionError(
                    "set [" + set +
                    "] has been merged: it should not be in the list");
            }
            for (Iterator subsets = set.subsets.iterator(); subsets.hasNext();) {
                RelSubset subset = (RelSubset) subsets.next();
                if (subset.set != set) {
                    throw new AssertionError(
                        "subset [" + subset.getDescription()
                        + "] is in wrong set [" + set + "]");
                }
                for (Iterator rels = subset.rels.iterator(); rels.hasNext();) {
                    RelNode rel = (RelNode) rels.next();
                    final RelSubset subset2 = getSubset(rel);
                    if ((subset2 != subset) && false) {
                        throw new AssertionError(
                            "rel [" + rel.getDescription()
                            + "] is in wrong subset [" + subset2 + "]");
                    }
                    final RelNode [] inputRels = rel.getInputs();
                    for (int i = 0; i < inputRels.length; i++) {
                        RelNode inputRel = inputRels[i];
                        final RelSubset inputSubset = getSubset(inputRel);
                        if (!inputSubset.parents.contains(rel)) {
                            throw new AssertionError(
                                "rel [" + rel.getDescription()
                                + "] is a parent of ["
                                + inputRel.getDescription()
                                + "] but is not registered as such");
                        }
                    }
                    RelOptCost relCost = getCost(rel);
                    if (relCost.isLt(subset.bestCost)) {
                        throw new AssertionError(
                            "rel [" + rel.getDescription()
                            + "] has lower cost " + relCost
                            + " than best cost " + subset.bestCost
                            + " of subset [" + subset.getDescription() + "]");
                    }
                }
            }
        }
    }

    public void registerAbstractRelationalRules()
    {
        //
        addRule(new AbstractConverter.ExpandConversionRule());
        addRule(new SwapJoinRule());
        addRule(new RemoveDistinctRule());
        addRule(new UnionToDistinctRule());
        addRule(new RemoveTrivialProjectRule());

        // todo: rule which makes Project({OrdinalRef}) disappear
    }

    public void registerSchema(RelOptSchema schema)
    {
        if (registeredSchemas.add(schema)) {
            try {
                schema.registerRules(this);
            } catch (Exception e) {
                throw Util.newInternal(e,
                    "Error while registering schema " + schema);
            }
        }
    }

    public JavaRelImplementor getJavaRelImplementor(RelNode rel)
    {
        return new JavaRelImplementor(
            rel.getCluster().getRexBuilder(),
            OJRexImplementorTableImpl.instance());
    }

    /**
     * Finds the cost of a node. Similar to {@link #optimize}, but does not
     * create any expressions.
     */
    public RelOptCost getCost(RelNode rel)
    {
        assert rel != null : "pre-condition: rel != null";
        if (rel instanceof RelSubset) {
            return ((RelSubset) rel).bestCost;
        }
        if (rel.getTraits().getTrait(0) == CallingConvention.NONE) {
            return makeInfiniteCost();
        }
        RelOptCost cost = RelMetadataQuery.getNonCumulativeCost(rel);
        if (!VolcanoCost.ZERO.isLt(cost)) {
            // cost must be positive, so nudge it
            cost = makeTinyCost();
        }
        RelNode [] inputs = rel.getInputs();
        for (int i = 0, n = inputs.length; i < n; i++) {
            cost = cost.plus(getCost(inputs[i]));
        }
        return cost;
    }

    /**
     * Returns the subset that a relational expression belongs to.
     *
     * @param rel Relational expression
     * @return Subset it belongs to, or null if it is not registered
     * @pre rel != null
     */
    RelSubset getSubset(RelNode rel)
    {
        assert rel != null : "pre: rel != null";
        if (rel instanceof RelSubset) {
            return (RelSubset) rel;
        } else {
            return mapRel2Subset.get(rel);
        }
    }

    RelSubset getSubset(
        RelNode rel,
        RelTraitSet traits)
    {
        if (rel instanceof RelSubset
                && (((RelSubset) rel).getTraits().equals(traits))) {
            return (RelSubset) rel;
        }
        RelSet set = getSet(rel);
        if (set == null) {
            return null;
        }
        return set.getSubset(traits);
    }

    private RelNode changeTraitsUsingConverters(
        RelNode rel, RelTraitSet toTraits, boolean allowAbstractConverters)
    {
        final RelTraitSet fromTraits = rel.getTraits();

        assert(fromTraits.size() >= toTraits.size());

        final boolean allowInfiniteCostConverters =
            SaffronProperties.instance().allowInfiniteCostConverters.get();

        // Naive algorithm: assumes that conversion from Tx1.Ty1 to Tx2.Ty2
        // can happen in order (e.g. the traits are completely orthogonal).
        // Also, toTraits may have fewer traits than fromTraits, excess traits
        // will be left as is.  Finally, any null entries in toTraits are
        // ignored.
        RelNode converted = rel;
        for (int i = 0; converted != null && i < toTraits.size(); i++) {
            RelTrait fromTrait = fromTraits.getTrait(i);
            RelTrait toTrait = toTraits.getTrait(i);

            if (toTrait == null) {
               continue;
            }

            assert(fromTrait.getTraitDef() == toTrait.getTraitDef());

            if (fromTrait == toTrait) {
                // No need to convert, it's already correct.
                continue;
            }

            rel = fromTrait.getTraitDef().convert(
                this, converted, toTrait, allowInfiniteCostConverters);
            if (rel == null && allowAbstractConverters) {
                RelTraitSet stepTraits =
                    RelOptUtil.clone(converted.getTraits());
                stepTraits.setTrait(toTrait.getTraitDef(), toTrait);

                rel =
                    new AbstractConverter(
                        converted.getCluster(), converted,
                        toTrait.getTraitDef(), stepTraits);
            }

            converted = rel;
        }

        return converted;
    }

    RelNode changeTraitsUsingConverters(
        RelNode rel,
        RelTraitSet toTraits)
    {
        return changeTraitsUsingConverters(rel, toTraits, false);
    }

    void checkForSatisfiedConverters(
        RelSet set,
        RelNode rel)
    {
        if (!set.abstractConverters.isEmpty()) {
            int i = 0;
            while (i < set.abstractConverters.size()) {
                AbstractConverter converter =
                    (AbstractConverter) set.abstractConverters.get(i);
                RelNode converted =
                    changeTraitsUsingConverters(rel,
                        converter.getTraits());
                if (converted == null) {
                    i++; // couldn't convert this; move on to the next
                } else {
                    if (!isRegistered(converted)) {
                        registerImpl(converted, set);
                    }
                    set.abstractConverters.remove(converter); // success
                }
            }
        }
    }

    void dump(PrintWriter pw)
    {
        pw.println("Root: " + root.getDescription());
        pw.println("Sets:");
        RelSet [] sets =
            (RelSet []) allSets.toArray(new RelSet[allSets.size()]);
        Arrays.sort(
            sets,
            new Comparator() {
                public int compare(
                    Object o1,
                    Object o2)
                {
                    return ((RelSet) o1).id - ((RelSet) o2).id;
                }
            });
        for (int i = 0; i < sets.length; i++) {
            RelSet set = sets[i];
            pw.println("Set#" + set.id);
            for (int j = 0; j < set.subsets.size(); j++) {
                RelSubset subset = (RelSubset) set.subsets.get(j);
                pw.println("\t" + subset.getDescription() + ", best="
                    + ((subset.best == null) ? "null"
                    : ("Rel#" + subset.best.getId())) + ", importance="
                    + ruleQueue.getImportance(subset));
                assert (subset.set == set);
                for (int k = 0; k < j; k++) {
                    assert (!((RelSubset) set.subsets.get(k)).getTraits()
                                .equals(subset.getTraits()));
                }
                for (int k = 0; k < subset.rels.size(); k++) {
                    RelNode rel = (RelNode) subset.rels.get(k);

                    // "\t\trel#34:JavaProject(Rel#32:JavaFilter(...), ...)"
                    pw.print("\t\t" + rel.getDescription());
                    RelNode [] inputs = rel.getInputs();
                    for (int m = 0; m < inputs.length; m++) {
                        RelNode input = inputs[m];
                        RelSubset inputSubset =
                            getSubset(
                                input,
                                input.getTraits());
                        RelSet inputSet = inputSubset.set;
                        if (input instanceof RelSubset) {
                            assert (inputSubset.rels.size() > 0);
                            input = (RelNode) inputSubset.rels.get(0);
                            assert (inputSubset.getTraits().equals(input
                                .getTraits()));
                            assert (inputSet.rels.contains(input));
                            assert (inputSet.subsets.contains(inputSubset));
                        }
                    }
                    pw.print(", rowcount=" + RelMetadataQuery.getRowCount(rel));
                    pw.println(", cumulative cost=" + getCost(rel));
                }
            }
        }
        pw.println();
    }

    void rename(RelNode rel)
    {
        final String oldDigest = rel.getDigest();
        if (fixupInputs(rel)) {
            assert mapDigestToRel.remove(oldDigest) == rel;
            final String newDigest = rel.recomputeDigest();
            tracer.finer("Rename #" + rel.getId() + " from '" + oldDigest
                + "' to '" + newDigest + "'");
            final RelNode equivRel = mapDigestToRel.put(newDigest, rel);
            if (equivRel != null) {
                assert equivRel != rel;

                // There's already an equivalent with the same name, and we
                // just knocked it out. Put it back, and forget about 'rel'.
                tracer.finer("After renaming rel#" + rel.getId()
                    + ", it is now equivalent to rel#" + equivRel.getId());
                mapDigestToRel.put(
                    equivRel.getDigest(),
                    equivRel);
                if (ruleQueue.remove(rel) && !ruleQueue.contains(equivRel)) {
                    ruleQueue.add(equivRel);
                }

                // Remove backlinks from children.
                final RelNode [] inputs = rel.getInputs();
                for (int i = 0; i < inputs.length; i++) {
                    RelSubset input = (RelSubset) inputs[i];
                    input.parents.remove(rel);
                }

                // Remove rel from its subset. (This may leave the subset
                // empty, but if so, that will be dealt with when the sets
                // get merged.)
                final RelSubset subset = mapRel2Subset.remove(rel);
                assert subset != null;
                boolean existed = subset.rels.remove(rel);
                assert existed : "rel was not known to its subset";
                existed = subset.set.rels.remove(rel);
                assert existed : "rel was not known to its set";
                final RelSubset equivSubset = getSubset(equivRel);
                if (equivSubset != subset) {
                    // The equivalent relational expression is in a different
                    // subset, therefore the sets are equivalent.
                    assert equivSubset.getTraits().equals(subset.getTraits());
                    assert equivSubset.set != subset.set;
                    merge(equivSubset.set, subset.set);
                }
            }
        }
    }

    void reregister(
        RelSet set,
        RelNode rel)
    {
        // Is there an equivalent relational expression? (This might have
        // just occurred because the relational expression's child was just
        // found to be equivalent to another set.)
        RelNode equivRel = mapDigestToRel.get(rel.getDigest());
        if ((equivRel != null) && (equivRel != rel)) {
            assert (equivRel.getClass() == rel.getClass());
            assert (equivRel.getTraits().equals(rel.getTraits()));
            if (ruleQueue.contains(rel)) {
                if (!ruleQueue.contains(equivRel)) {
                    ruleQueue.add(equivRel);
                }
                ruleQueue.remove(rel);
            }
            return;
        }

        // Add the relational expression into the correct set and subset.
        RelSubset subset2 = set.add(rel);
        mapRel2Subset.put(rel, subset2);
    }

    private RelSubset canonize(final RelSubset subset)
    {
        if (subset.set.equivalentSet == null) {
            return subset;
        }
        RelSet set = subset.set;
        do {
            set = set.equivalentSet;
        } while (set.equivalentSet != null);
        return set.getOrCreateSubset(
            subset.getCluster(),
            subset.getTraits());
    }

    private RelSubset findBestPlan_old(
        RelSubset subset,
        RelOptCost targetCost)
    {
        if (subset.active) {
            return subset; // prevent cycles
        }
        if (subset.getTraits().getTrait(0) == CallingConvention.NONE) {
            return subset; // don't even bother
        }
        subset.active = true;
        for (int i = 0; i < subset.rels.size(); i++) {
            RelNode rel = (RelNode) subset.rels.get(i);
            assert (rel.getTraits().equals(subset.getTraits()));
            RelOptCost minCost = targetCost;
            if (subset.bestCost.isLt(minCost)) {
                // not enough to do better than our target -- we have to do better than
                // the best we already have
                minCost = subset.bestCost;
            }
            RelOptCost cost = optimize(rel, minCost);
            if (cost.isLt(minCost)) {
                subset.best = rel;
                subset.bestCost = cost;

                // Lower cost means lower importance. Other nodes will change
                // too, but we'll get to them later.
                ruleQueue.recompute(subset);
            }
        }
        subset.active = false;

        /*
           // also consider other subsets of the same set, if they can be
           // converted to this convention
           RelSet set = subset.set;
           int found = 0;
           for (int i = 0; i < set.subsets.size(); i++) {
               RelSubset subset2 = (RelSubset) set.subsets.get(i);
               if (subset2 == subset) {
                   continue;
               }
               if (Converter.canConvertIndirectly(subset2.convention, subset.convention)) {
                   if (subset2.bestCost.isInfinite()) {
                       findBestPlan_old(subset2, subset.bestCost);
                   }
                   if (subset2.bestCost.isLt(subset.bestCost)) {
                       Rel converter = Converter.create(
                               subset.getCluster(), subset2,
                               subset2.getConvention(), subset.getConvention());
                       if (lookup(converter) == null) {
                           // Converter did not previously exist. We've done
                           // something useful.
                           register(converter, set, Planner.RegisterFlag.MAY_BE_REGISTERED);
                           found++;
                       }
                   }
               }
           }
           if (found > 0) {
               // now we have more options, recursively invoke ourselves to see
               // if we can do better
               findBestPlan_old(subset, subset.bestCost);
           }
         */
        return canonize(subset);
    }

    /**
     * Fires all rules matched by a relational expression.
     *
     * @param rel Relational expression which has just been created (or maybe
     *        from the queue)
     * @param deferred If true, each time a rule matches, just add an entry to
     *        the queue.
     */
    void fireRules(
        RelNode rel,
        boolean deferred)
    {
        for (RelOptRuleOperand operand : allOperands) {
            if (operand.matches(rel)) {
                final VolcanoRuleCall ruleCall;
                if (deferred) {
                    ruleCall = new DeferringRuleCall(this, operand);
                } else {
                    ruleCall = new VolcanoRuleCall(this, operand);
                }
                ruleCall.match(rel);
            }
        }
    }

    private void fireRulesForSubset(RelSubset childSubset)
    {
        while (true) {
            RelNode rel = ruleQueue.findCheapestMember(childSubset);
            if (rel == null) {
                break;
            }
            if (ruleQueue.remove(rel)) {
                fireRules(rel, false);
            }
        }
    }

    private boolean fixupInputs(RelNode rel)
    {
        int changeCount = 0;
        final RelNode [] inputs = rel.getInputs();
        for (int i = 0; i < inputs.length; i++) {
            RelNode input = inputs[i];
            if (input instanceof RelSubset) {
                final RelSubset subset = (RelSubset) input;
                RelSubset newSubset = canonize(subset);
                if (newSubset != subset) {
                    rel.replaceInput(i, newSubset);
                    subset.parents.remove(rel);
                    newSubset.parents.add(rel);
                    changeCount++;
                }
            }
        }
        return changeCount > 0;
    }

    private void merge(
        RelSet set,
        RelSet set2)
    {
        if (set == set2) {
            return;
        }
        if (set.id > set2.id) {
            // Swap the sets, so we're always merging the newer set into the
            // older.
            RelSet t = set;
            set = set2;
            set2 = t;
        }
        set.mergeWith(this, set2);
        if (set2 == getSet(root)) {
            root =
                set.getOrCreateSubset(
                    root.getCluster(),
                    root.getTraits());
        }
    }

    /**
     * By optimizing its children, finds the best implementation of relational
     * expression <code>rel</code>.  The cost is bounded by
     * <code>targetCost</code>.
     */
    private RelOptCost optimize(
        RelNode rel,
        RelOptCost targetCost)
    {
loop:
        while (true) {
            // First, try to do the node itself.
            RelOptCost nodeCost = RelMetadataQuery.getNonCumulativeCost(rel);
            if (!nodeCost.isLt(targetCost)) {
                int beforeCount = registerCount;
                if (ruleQueue.remove(rel)) {
                    fireRules(rel, false);
                }
                if (registerCount > beforeCount) {
                    continue loop;
                }
                tracer.finer("Optimize: cannot implement [" +
                    rel.getDescription() + "] in less than [" +
                    targetCost + "]");
                return makeInfiniteCost(); // no can do
            }

            RelOptCost usedCost = nodeCost;

            // Second, figure out if we can do the children using the remaining
            // resources.
            RelNode [] inputs = rel.getInputs();
            for (int j = 0; j < inputs.length; j++) {
                // Because exp is registered, each relational child is a
                // RelSubset.
                RelOptCost remainingCost = targetCost.minus(usedCost);
                RelSubset childSubset =
                    findBestPlan_old((RelSubset) inputs[j], remainingCost);

                // Use RelSubset.bestCost, not Rel.getCost(), because (a) it
                // includes children, (b) it prevents cycles during optimize, (c)
                // it potentially prevents expensive cost calculations on deep
                // trees.
                if (!childSubset.bestCost.isLt(remainingCost)) {
                    int beforeCount = registerCount;
                    fireRulesForSubset(childSubset);
                    if (registerCount > beforeCount) {
                        continue loop;
                    }
                    tracer.finer(
                        "Optimize: cannot implement2 " + rel.getDescription() +
                        ", cost=" + childSubset.bestCost);
                    return makeInfiniteCost(); // no can do
                }
                usedCost = usedCost.plus(childSubset.bestCost);
            }

            tracer.finer("Optimize: rel=" + rel.getId() + ", cost=" + usedCost);
            return usedCost;
        }
    }

    /**
     * Registers a new expression <code>exp</code> and queues up rule matches.
     * If <code>set</code> is not null, makes the expression part of that
     * equivalence set.  If an identical expression is already registered,
     * we don't need to register this one and nor should we queue up rule
     * matches.
     *
     * @param rel relational expression to register.
     *   Must be either a {@link RelSubset}, or an unregistered {@link RelNode}
     * @param set set that rel belongs to, or <code>null</code>
     *
     * @return the equivalence-set
     *
     * @pre rel instanceof RelSubset || !isRegistered(rel)
     */
    private RelSubset registerImpl(
        RelNode rel,
        RelSet set)
    {
        assert rel instanceof RelSubset || !isRegistered(rel) :
            "pre: rel instanceof RelSubset || !isRegistered(rel)" +
            " : {rel=" + rel + "}";
        if (rel instanceof RelSubset) {
            return registerSubset(set, (RelSubset) rel);
        }

        if (rel.getCluster().getPlanner() != this) {
            throw Util.newInternal("Relational expression " + rel +
                " belongs to a different planner than is currently being" +
                " used.");
        }

        // Now is a good time to ensure that the relational expression
        // implements the interface required by its calling convention.
        final RelTraitSet traits = rel.getTraits();
        final CallingConvention convention =
            (CallingConvention) traits.getTrait(0);
        if (!convention.getInterface().isInstance(rel) &&
            !(rel instanceof ConverterRel)) {
            throw Util.newInternal("Relational expression " + rel
                + " has calling-convention " + convention
                + " but does not implement the required interface '"
                + convention.getInterface() + "' of that convention");
        }
        if (traits.size() != traitDefs.size()) {
            throw Util.newInternal("Relational expression " + rel
                + " does not have the correct number of traits");
        }

        // Ensure that its sub-expressions are registered.
        rel.onRegister(this);

        // If it is equivalent to an existing expression, return the set that
        // the equivalent expression belongs to.
        String digest = rel.getDigest();
        RelNode equivExp = mapDigestToRel.get(digest);
        if (equivExp == null) {
            ;
        } else if (equivExp == rel) {
            return getSubset(rel);
        } else {
            assert (equivExp.getTraits().equals(traits)
                && (equivExp.getClass() == rel.getClass()));
            RelSet equivSet = getSet(equivExp);
            if (equivSet != null) {
                tracer.finer("Register: rel#" + rel.getId()
                    + " is equivalent to " + equivExp.getDescription());
                return registerSubset(
                    set,
                    getSubset(equivExp));
            }
        }

        // Converters are in the same set as their children.
        if (rel instanceof ConverterRel) {
            final RelNode input = ((ConverterRel) rel).getChild();
            final RelSet childSet = getSet(input);
            if ((set != null) && (set != childSet)
                    && (set.equivalentSet == null)) {
                tracer.finer("Register #" + rel.getId() + " " + digest
                    + " (and merge sets, because it is a conversion)");
                merge(set, childSet);
                registerCount++;

                // During the mergers, the child set may have changed, and since
                // we're not registered yet, we won't have been informed. So
                // check whether we are now equivalent to an existing
                // expression.
                if (fixupInputs(rel)) {
                    digest = rel.recomputeDigest();
                    RelNode equivRel = mapDigestToRel.get(digest);
                    if ((equivRel != rel) && (equivRel != null)) {
                        // There is already an equivalent expression. Use that
                        // one, and forget about this one.
                        return getSubset(equivRel);
                    }
                }
            } else {
                set = childSet;
            }
        }

        // Place the expression in the appropriate equivalence set.
        if (set == null) {
            set = new RelSet();
            set.id = nextSetId++;
            set.variablesPropagated =
                Util.minus(
                    RelOptUtil.getVariablesSet(rel),
                    rel.getVariablesStopped());
            set.variablesUsed = RelOptUtil.getVariablesUsed(rel);
            this.allSets.add(set);
        }

        // Chain to find 'live' equivalent set, just in case several sets are
        // merging at the same time.
        while (set.equivalentSet != null) {
            set = set.equivalentSet;
        }
        registerCount++;
        RelSubset subset = set.add(rel);
        mapRel2Subset.put(rel, subset);
        final RelNode xx = mapDigestToRel.put(digest, rel);
        assert ((xx == null) || (xx == rel));
        tracer.finer(
            "Register " + rel.getDescription() +
            " in " + subset.getDescription());

        // This relational expression may have been registered while we
        // recursively registered its children. If this is the case, we're done.
        if (xx != null) {
            return subset;
        }

        // Create back-links from its children, which makes children more
        // important.
        if (rel == this.root) {
            ruleQueue.subsetImportances.put(
                subset,
                new Double(1.0)); // todo: remove
        }
        RelNode [] inputs = rel.getInputs();
        for (int i = 0; i < inputs.length; i++) {
            RelSubset childSubset = (RelSubset) inputs[i];
            childSubset.parents.add(rel);

            // Child subset is more important now a new parent uses it.
            ruleQueue.recompute(childSubset);
        }
        if (rel == this.root) {
            ruleQueue.subsetImportances.remove(subset);
        }

        // Remember abstract converters until they're satisfied
        if (rel instanceof AbstractConverter) {
            set.abstractConverters.add(rel);
        }

        // If this set has any unsatisfied converters, try to satisfy them.
        checkForSatisfiedConverters(set, rel);

        // Add relational expression to queue of expressions whose rules
        // have not been fired.
        ruleQueue.add(rel);

        // Queue up all rules triggered by this relexp's creation.
        fireRules(rel, true);

        return subset;
    }

    private RelSubset registerSubset(
        RelSet set,
        RelSubset subset)
    {
        if (set != subset.set &&
            set != null &&
            set.equivalentSet == null &&
            subset.set.equivalentSet == null)
        {
            tracer.finer("Register #" + subset.getId() + " " + subset
                + ", and merge sets");
            merge(set, subset.set);
            registerCount++;
        }
        return subset;
    }

    // implement RelOptPlanner
    public void addListener(RelOptListener newListener)
    {
        // TODO jvs 6-Apr-2006:  new superclass AbstractRelOptPlanner
        // now defines a multicast listener; just need to hook it in
        if (listener != null) {
            throw Util.needToImplement("multiple VolcanoPlanner listeners");
        }
        listener = newListener;
    }

    // implement RelOptPlanner
    public void registerMetadataProviders(ChainedRelMetadataProvider chain)
    {
        chain.addProvider(
            new VolcanoRelMetadataProvider());
    }

    // implement RelOptPlanner
    public long getRelMetadataTimestamp(RelNode rel)
    {
        RelSubset subset = getSubset(rel);
        if (subset == null) {
            return 0;
        } else {
            return subset.timestamp;
        }
    }
    
    //~ Inner Classes ---------------------------------------------------------

    /**
     * A rule call which defers its actions. Whereas {@link RelOptRuleCall}
     * invokes the rule when it finds a match, a
     * <code>DeferringRuleCall</code> creates a {@link VolcanoRuleMatch}
     * which can be invoked later.
     */
    private static class DeferringRuleCall extends VolcanoRuleCall
    {
        DeferringRuleCall(
            VolcanoPlanner planner,
            RelOptRuleOperand operand)
        {
            super(planner, operand);
        }

        /**
         * Rather than invoking the rule (as the base method does), creates a
         * {@link VolcanoRuleMatch} which can be invoked later.
         */
        protected void onMatch()
        {
            final VolcanoRuleMatch match =
                new VolcanoRuleMatch(volcanoPlanner, getOperand0(), rels);
            volcanoPlanner.ruleQueue.addMatch(match);
        }
    }
}


// End VolcanoPlanner.java
