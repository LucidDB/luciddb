/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.opt;

import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.core.SaffronSchema;
import net.sf.saffron.oj.rel.JavaRelImplementor;
import net.sf.saffron.rel.*;
import net.sf.saffron.rel.convert.ConverterRel;
import net.sf.saffron.rel.convert.ConverterRule;
import net.sf.saffron.util.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.logging.Level;


/**
 * VolcanoPlanner optimizes queries by transforming expressions selectively
 * according to a dynamic programming algorithm.
 */
public class VolcanoPlanner implements SaffronPlanner
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
    ArrayList allOperands = new ArrayList();

    /** List of all sets. Used only for debugging. */
    ArrayList allSets = new ArrayList();

    /**
     * Canonical map from {@link String digest} to the unique {@link
     * SaffronRel relational expression} with that digest.
     */
    HashMap mapDigestToRel = new HashMap();

    /**
     * Map each registered expression ({@link SaffronRel}) to its equivalence
     * set ({@link RelSubset}).
     *
     * <p>We use an {@link IdentityHashMap} to simplify the process of
     * merging {@link RelSet} objects. Most {@link SaffronRel} objects are
     * identified by their digest, which involves the set that their child
     * relational expressions belong to. If those children belong to the same
     * set, we have to be careful, otherwise it gets incestuous.</p>
     */
    IdentityHashMap mapRel2Subset = new IdentityHashMap();

    /** List of all schemas which have been registered. */
    HashSet registeredSchemas = new HashSet();

    /** Holds rule calls waiting to be fired. */
    RuleQueue ruleQueue = new RuleQueue(this);
    private final ArrayList callingConventions = new ArrayList();
    private final Graph conversionGraph = new Graph();

    /**
     * Set of all registered rules.
     */
    private final HashSet ruleSet = new HashSet();

    /**
     * Maps rule description to rule, just to ensure that rules' descriptions
     * are unique.
     */
    private final HashMap mapDescToRule = new HashMap();

    /**
     * For a given source/target convention, there may be several possible
     * conversion rules. Maps {@link Graph.Arc} to a collection of
     * {@link ConverterRule} objects.
     */
    private final MultiMap mapArcToConverterRule = new MultiMap();
    private int nextSetId = 0;

    /**
     * Incremented every time a relational expression is registered or two
     * sets are merged. Tells us whether anything is going on.
     */
    private int registerCount;

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

    // todo: pre-compute
    public RuleOperand [] getConversionOperands(
        CallingConvention toConvention)
    {
        ArrayList list = new ArrayList();
        for (int i = 0,count = allOperands.size(); i < count; i++) {
            RuleOperand operand = (RuleOperand) allOperands.get(i);
            if (operand.rule.getOutConvention() == toConvention) {
                list.add(operand);
            }
        }
        return (RuleOperand []) list.toArray(RuleOperand.noOperands);
    }

    public boolean isRegistered(SaffronRel rel)
    {
        return mapRel2Subset.get(rel) != null;
    }

    public void setRoot(SaffronRel rel)
    {
        this.root = registerImpl(rel,null);

        // Making a node the root changes its importance.
        this.ruleQueue.recompute(this.root);
    }

    public SaffronRel getRoot()
    {
        return root;
    }

    /**
     * Find an expression's equivalence set.  If the expression is not
     * registered, return null.
     */
    public RelSet getSet(SaffronRel rel)
    {
        final RelSubset subset = getSubset(rel);
        if (subset != null) {
            assert subset.set != null;
            return subset.set;
        }
        return null;
    }

    public boolean addCallingConvention(CallingConvention convention)
    {
        if (callingConventions.contains(convention)) {
            return false;
        }
        callingConventions.add(convention);
        return true;
    }

    public boolean addRule(VolcanoRule rule)
    {
        if (ruleSet.contains(rule)) {
            // Rule already exists.
            return false;
        }
        final boolean added = ruleSet.add(rule);
        assert added;
        // Check that there isn't a rule with the same description.
        final String description = rule.toString();
        assert(description != null);
        assert(description.indexOf("$") < 0) :
            "Rule's description must not contain '$'";
        VolcanoRule existingRule =
            (VolcanoRule) mapDescToRule.put(description,rule);
        if (existingRule != null) {
            if (existingRule == rule) {
                throw new AssertionError("Rule not already registered");
            } else {
                // This rule has the same description as one previously
                // registered, yet it is not equal. You may need to fix the
                // rule's equals and hashCode methods.
                throw new AssertionError("Rule's description is unique; " +
                        "existing rule=" + existingRule +
                        "; new rule=" + rule);
            }
        }

        // Each of this rule's operands is an 'entry point' for a rule call.
        Walker operandWalker = new Walker(rule.operand);
        int ordinalInRule = 0;
        ArrayList operandsOfRule = new ArrayList();
        while (operandWalker.hasMoreElements()) {
            RuleOperand operand = (RuleOperand) operandWalker.nextElement();
            operand.rule = rule;
            operand.parent = (RuleOperand) operandWalker.getParent();
            operand.ordinalInParent = operandWalker.getOrdinal();
            operand.ordinalInRule = ordinalInRule++;
            operandsOfRule.add(operand);
            allOperands.add(operand);
        }

        // Convert this rule's operands from a list to an array.
        rule.operands =
            (RuleOperand []) operandsOfRule.toArray(RuleOperand.noOperands);

        // Build each operand's solve-order.  Start with itself, then its
        // parent, up to the root, then the remaining operands in prefix
        // order.
        for (int j = 0; j < rule.operands.length; j++) {
            RuleOperand operand = rule.operands[j];
            operand.solveOrder = new int[rule.operands.length];
            int m = 0;
            for (RuleOperand o = operand; o != null; o = o.parent) {
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
            assert(m == rule.operands.length);
        }

        // If this is a converter rule, add it to the conversion graph.
        if (rule instanceof ConverterRule) {
            ConverterRule converterRule = (ConverterRule) rule;
            if (converterRule.isGuaranteed()) {
                final Graph.Arc arc =
                    conversionGraph.createArc(
                        converterRule.inConvention,
                        converterRule.outConvention);
                mapArcToConverterRule.putMulti(arc,rule);
            }
        }

        return true;
    }

    public boolean canConvert(
        CallingConvention fromConvention,
        CallingConvention toConvention)
    {
        return conversionGraph.getShortestPath(fromConvention,toConvention) != null;
    }

    public SaffronRel changeConvention(
        final SaffronRel rel,
        CallingConvention toConvention)
    {
        assert(rel.getConvention() != toConvention);
        RelSubset rel2 = registerImpl(rel,null);
        if (rel2.convention == toConvention) {
            return rel2;
        }
        SaffronRel rel3 = changeConventionUsingConverters(rel2,toConvention);
        if (rel3 != null) {
            return rel3;
        }
        final AbstractConverter converter =
            new AbstractConverter(rel.getCluster(),rel,toConvention);
        return register(converter,rel);
    }

    public SaffronPlanner chooseDelegate()
    {
        return this;
    }

    public SaffronRel findBestExp()
    {
        PlanCost targetCost = makeHugeCost();
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
    public PlanCost makeCost(double dRows,double dCpu,double dIo)
    {
        return new VolcanoCost(dRows,dCpu,dIo);
    }

    public PlanCost makeHugeCost()
    {
        return VolcanoCost.HUGE;
    }

    public PlanCost makeInfiniteCost()
    {
        return VolcanoCost.INFINITY;
    }

    public PlanCost makeTinyCost()
    {
        return VolcanoCost.TINY;
    }

    public PlanCost makeZeroCost()
    {
        return VolcanoCost.ZERO;
    }

    public SaffronRel register(SaffronRel rel,SaffronRel equivRel)
    {
        final RelSet set = getSet(equivRel);
        final RelSubset subset = registerImpl(rel,set);
        if (true) {
            validate();
        }
        return subset;
    }

    /**
     * Checks internal consistency.
     */
    private void validate() {
        for (Iterator sets = allSets.iterator(); sets.hasNext();) {
            RelSet set = (RelSet) sets.next();
            if (set.equivalentSet != null) {
                throw new AssertionError("set [" + set +
                        "] has been merged: it should not be in the list");
            }
            for (Iterator subsets = set.subsets.iterator(); subsets.hasNext();) {
                RelSubset subset = (RelSubset) subsets.next();
                if (subset.set != set) {
                    throw new AssertionError("subset [" + subset +
                            "] is in wrong set [" + set + "]");
                }
                for (Iterator rels = subset.rels.iterator(); rels.hasNext();) {
                    SaffronRel rel = (SaffronRel) rels.next();
                    final RelSubset subset2 = getSubset(rel);
                    if (subset2 != subset && false) {
                        throw new AssertionError("rel [" + rel +
                                "] is in wrong subset [" + subset2 + "]");
                    }
                    final SaffronRel [] inputRels = rel.getInputs();
                    for (int i = 0; i < inputRels.length; i++) {
                        SaffronRel inputRel = inputRels[i];
                        final RelSubset inputSubset = getSubset(inputRel);
                        if (!inputSubset.parents.contains(rel)) {
                            throw new AssertionError("rel [" + rel +
                                    "] is a parent of [" + inputRel +
                                    "] but is not registered as such");
                        }
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

    public void registerAbstractRels()
    {
        AggregateRel.register(this);
        DistinctRel.register(this);
        FilterRel.register(this);
        JoinRel.register(this);
        OneRowRel.register(this);
        ProjectRel.register(this);
        TableAccessRel.register(this);
        UnionRel.register(this);
        CalcRel.register(this);
        addRule(FilterToCalcRule.instance);
        addRule(ProjectToCalcRule.instance);
        addRule(MergeFilterOntoCalcRule.instance);
        addRule(MergeProjectOntoCalcRule.instance);
    }

    public void registerSchema(SaffronSchema schema)
    {
        if (registeredSchemas.add(schema)) {
            try {
                schema.registerRules(this);
            } catch (Exception e) {
                throw Util.newInternal(
                    e,
                    "Error while registering schema " + schema);
            }
        }
    }


    public JavaRelImplementor getJavaRelImplementor(SaffronRel rel)
    {
        return new JavaRelImplementor(rel.getCluster().rexBuilder);
    }


    /**
     * Finds the cost of a node. Similar to {@link #optimize}, but does not
     * create any expressions.
     */
    PlanCost getCost(SaffronRel rel)
    {
        if (rel instanceof RelSubset) {
            return ((RelSubset) rel).bestCost;
        }
        if (rel.getConvention() == CallingConvention.NONE) {
            return makeInfiniteCost();
        }
        PlanCost cost = rel.computeSelfCost(this);
        if (!VolcanoCost.ZERO.isLt(cost)) {
            throw Util.newInternal(
                "Cost " + cost + " of " + rel + " must be positive.");
        }
        SaffronRel [] inputs = rel.getInputs();
        for (int i = 0,n = inputs.length; i < n; i++) {
            cost = cost.plus(getCost(inputs[i]));
        }
        return cost;
    }

    RelSubset getSubset(SaffronRel rel)
    {
        if (rel instanceof RelSubset) {
            return (RelSubset) rel;
        } else {
            return (RelSubset) mapRel2Subset.get(rel);
        }
    }

    RelSubset getSubset(SaffronRel rel,CallingConvention convention)
    {
        if (
            rel instanceof RelSubset
                && (((RelSubset) rel).convention == convention)) {
            return (RelSubset) rel;
        }
        RelSet set = getSet(rel);
        if (set == null) {
            return null;
        }
        return set.getSubset(convention);
    }

    SaffronRel changeConventionUsingConverters(
        SaffronRel rel,
        CallingConvention toConvention)
    {
        final CallingConvention fromConvention = rel.getConvention();
        Iterator conversionPaths =
            conversionGraph.getPaths(fromConvention,toConvention);
        boolean allowInfiniteCostConverters =
                SaffronProperties.instance().allowInfiniteCostConverters.get();
        loop: while (conversionPaths.hasNext()) {
            Graph.Arc [] arcs = (Graph.Arc []) conversionPaths.next();
            assert(arcs[0].from == fromConvention);
            assert(arcs[arcs.length - 1].to == toConvention);
            SaffronRel converted = rel;
            for (int i = 0; i < arcs.length; i++) {
                if (getCost(converted).isInfinite() &&
                        !allowInfiniteCostConverters) {
                    continue loop;
                }
                converted = changeConvention(converted,arcs[i]);
                if (converted == null) {
                    throw Util.newInternal(
                        "Converter from " + arcs[i].from + " to " + arcs[i].to
                        + " guaranteed that it could convert any relexp");
                }
            }
            return converted;
        }
        return null;
    }

    void checkForSatisfiedConverters(RelSet set,SaffronRel rel)
    {
        if (!set.abstractConverters.isEmpty()) {
            int i = 0;
            while (i < set.abstractConverters.size()) {
                AbstractConverter converter =
                    (AbstractConverter) set.abstractConverters.get(i);
                SaffronRel converted =
                    changeConventionUsingConverters(
                        rel,
                        converter.outConvention);
                if (converted == null) {
                    i++; // couldn't convert this; move on to the next
                } else {
                    registerImpl(converted,set);
                    set.abstractConverters.remove(converter); // success
                }
            }
        }
    }

    void dump(PrintWriter pw)
    {
        pw.println("Root: " + root);
        pw.println("Sets:");
        RelSet [] sets =
            (RelSet []) allSets.toArray(new RelSet[allSets.size()]);
        Arrays.sort(
            sets,
            new Comparator() {
                public int compare(Object o1,Object o2)
                {
                    return ((RelSet) o1).id - ((RelSet) o2).id;
                }
            });
        for (int i = 0; i < sets.length; i++) {
            RelSet set = sets[i];
            pw.println("Set#" + set.id);
            for (int j = 0; j < set.subsets.size(); j++) {
                RelSubset subset = (RelSubset) set.subsets.get(j);
                pw.println(
                    "\t" + subset.toString() + ", rel#" + subset.getId()
                    + ", best="
                    + ((subset.best == null) ? "null"
                                             : ("Rel#" + subset.best.getId()))
                    + ", importance=" + ruleQueue.getImportance(subset));
                assert(subset.set == set);
                for (int k = 0; k < j; k++) {
                    assert(
                        ((RelSubset) set.subsets.get(k)).getConvention() != subset
                            .getConvention());
                }
                for (int k = 0; k < subset.rels.size(); k++) {
                    SaffronRel rel = (SaffronRel) subset.rels.get(k);

                    // "\t\trel#34:JavaProject(Rel#32:JavaFilter(...), ...)"
                    pw.print(
                        "\t\t#" + rel.getId() + " " + rel.toString());
                    SaffronRel [] inputs = rel.getInputs();
                    for (int m = 0; m < inputs.length; m++) {
                        SaffronRel input = inputs[m];
                        RelSubset inputSubset =
                            getSubset(input,input.getConvention());
                        RelSet inputSet = inputSubset.set;
                        if (input instanceof RelSubset) {
                            assert(inputSubset.rels.size() > 0);
                            input = (SaffronRel) inputSubset.rels.get(0);
                            assert(
                                inputSubset.getConvention() == input
                                    .getConvention());
                            assert(inputSet.rels.contains(input));
                            assert(
                                inputSet.subsets.contains(inputSubset));
                        }
                    }
                    pw.println(", cost=" + getCost(rel));
                }
            }
        }
        pw.println();
    }

    void rename(SaffronRel rel)
    {
        final String oldDigest = rel.toString();
        if (fixupInputs(rel)) {
            assert(mapDigestToRel.remove(oldDigest) == rel);
            final String newDigest = rel.recomputeDigest();
            tracer.finer( "Rename #" + rel.getId() + " from '" + oldDigest +
                    "' to '" + newDigest + "'");
            final SaffronRel equivRel =
                (SaffronRel) mapDigestToRel.put(newDigest,rel);
            if (equivRel != null) {
                assert equivRel != rel;
                // There's already an equivalent with the same name, and we
                // just knocked it out. Put it back, and forget about 'rel'.
                tracer.finer("After renaming #" + rel.getId() +
                        ", it is now equivalent to rel #" + equivRel.getId());
                mapDigestToRel.put(equivRel.toString(),equivRel);
                if (ruleQueue.remove(rel) && !ruleQueue.contains(equivRel)) {
                    ruleQueue.add(equivRel);
                }
                // Remove backlinks from children.
                final SaffronRel[] inputs = rel.getInputs();
                for (int i = 0; i < inputs.length; i++) {
                    RelSubset input = (RelSubset) inputs[i];
                    input.parents.remove(rel);
                }
                // Remove rel from its subset. (This may leave the subset
                // empty, but if so, that will be dealt with when the sets
                // get merged.)
                final RelSubset subset = (RelSubset) mapRel2Subset.remove(rel);
                assert subset != null;
                boolean existed = subset.rels.remove(rel);
                assert existed : "rel was not known to its subset";
                existed = subset.set.rels.remove(rel);
                assert existed : "rel was not known to its set";
                final RelSubset equivSubset = getSubset(equivRel);
                if (equivSubset != subset) {
                    // The equivalent relational expression is in a different
                    // subset, therefore the sets are equivalent.
                    assert equivSubset.convention == subset.convention;
                    assert equivSubset.set != subset.set;
                    merge(equivSubset.set, subset.set);
                }
            }
        }
    }

    void reregister(RelSet set,SaffronRel rel)
    {
        // Is there an equivalent relational expression? (This might have
        // just occurred because the relational expression's child was just
        // found to be equivalent to another set.)
        SaffronRel equivRel = (SaffronRel) mapDigestToRel.get(rel.toString());
        if ((equivRel != null) && (equivRel != rel)) {
            assert(equivRel.getClass() == rel.getClass());
            assert(equivRel.getConvention() == rel.getConvention());
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
        return set.getOrCreateSubset(subset.getCluster(),subset.convention);
    }

    /**
     * Tries to convert a relational expression to the target convention of an
     * arc.
     */
    private SaffronRel changeConvention(SaffronRel rel,Graph.Arc arc)
    {
        assert(arc.from == rel.getConvention());

        // Try to apply each converter rule for this arc's source/target calling
        // conventions.
        for (
            Iterator converterRuleIter =
                mapArcToConverterRule.getMulti(arc).iterator();
                converterRuleIter.hasNext();) {
            ConverterRule converterRule =
                (ConverterRule) converterRuleIter.next();
            assert(converterRule.inConvention == arc.from);
            assert(converterRule.outConvention == arc.to);
            SaffronRel converted = converterRule.convert(rel);
            if (converted != null) {
                return converted;
            }
        }
        return null;
    }

    private RelSubset findBestPlan_old(RelSubset subset,PlanCost targetCost)
    {
        if (subset.active) {
            return subset; // prevent cycles
        }
        if (subset.convention == CallingConvention.NONE) {
            return subset; // don't even bother
        }
        subset.active = true;
        for (int i = 0; i < subset.rels.size(); i++) {
            SaffronRel rel = (SaffronRel) subset.rels.get(i);
            assert(rel.getConvention() == subset.convention);
            PlanCost minCost = targetCost;
            if (subset.bestCost.isLt(minCost)) {
                // not enough to do better than our target -- we have to do better than
                // the best we already have
                minCost = subset.bestCost;
            }
            PlanCost cost = optimize(rel,minCost);
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
     * Fire all rules matched by a relational expression.
     *
     * @param rel Relational expression which has just been created (or maybe
     *        from the queue)
     * @param deferred If true, each time a rule matches, just add an entry to
     *        the queue.
     */
    void fireRules(SaffronRel rel,boolean deferred)
    {
        for (int i = 0; i < allOperands.size(); i++) {
            RuleOperand operand = (RuleOperand) allOperands.get(i);
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
            SaffronRel rel = ruleQueue.findCheapestMember(childSubset);
            if (rel == null) {
                break;
            }
            if (ruleQueue.remove(rel)) {
                fireRules(rel,false);
            }
        }
    }

    private boolean fixupInputs(SaffronRel rel)
    {
        int changeCount = 0;
        final SaffronRel [] inputs = rel.getInputs();
        for (int i = 0; i < inputs.length; i++) {
            SaffronRel input = inputs[i];
            if (input instanceof RelSubset) {
                final RelSubset subset = (RelSubset) input;
                RelSubset newSubset = canonize(subset);
                if (newSubset != subset) {
                    rel.replaceInput(i,newSubset);
                    subset.parents.remove(rel);
                    newSubset.parents.add(rel);
                    changeCount++;
                }
            }
        }
        return changeCount > 0;
    }

    private void merge(RelSet set,RelSet set2)
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
        set.mergeWith(this,set2);
        if (set2 == getSet(root)) {
            root =
                set.getOrCreateSubset(root.getCluster(),root.getConvention());
        }
    }

    /**
     * By optimizing its children, find the best implementation of relational
     * expression <code>rel</code>.  The cost is bounded by
     * <code>targetCost</code>.
     */
    private PlanCost optimize(SaffronRel rel,PlanCost targetCost)
    {
loop:
        while (true) {
            // First, try to do the node itself.
            PlanCost nodeCost = rel.computeSelfCost(this);
            if (!nodeCost.isLt(targetCost)) {
                int beforeCount = registerCount;
                if (ruleQueue.remove(rel)) {
                    fireRules(rel,false);
                }
                if (registerCount > beforeCount) {
                    continue loop;
                }
                tracer.finer(
                    "Optimize: cannot implement [" + rel.toString()
                    + "] in less than [" + targetCost + "]");
                return makeInfiniteCost(); // no can do
            }

            PlanCost usedCost = nodeCost;

            // Second, figure out if we can do the children using the remaining
            // resources.
            SaffronRel [] inputs = rel.getInputs();
            for (int j = 0; j < inputs.length; j++) {
                // Because exp is registered, each relational child is a
                // RelSubset.
                PlanCost remainingCost = targetCost.minus(usedCost);
                RelSubset childSubset =
                    findBestPlan_old((RelSubset) inputs[j],remainingCost);

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
                        "Optimize: cannot implement2 " + rel.toString()
                        + ", cost=" + childSubset.bestCost);
                    return makeInfiniteCost(); // no can do
                }
                usedCost = usedCost.plus(childSubset.bestCost);
            }

            tracer.finer(
                "Optimize: rel=" + rel.getId() + ", cost=" + usedCost);
            return usedCost;
        }
    }

    /**
     * Register a new expression <code>exp</code>.  If <code>set</code> is not
     * null, make the expression part of that equivalence set.  If an
     * identical expression is already registered, we don't need to register
     * this one.
     *
     * @param rel relational expression to register
     * @param set set that rel belongs to, or <code>null</code>
     *
     * @return the equivalence-set
     */
    private RelSubset registerImpl(SaffronRel rel,RelSet set)
    {
        if (rel instanceof RelSubset) {
            return registerSubset(set,(RelSubset) rel);
        }

        // Now is a good time to ensure that the relational expression
        // implements the interface required by its calling convention.
        final CallingConvention convention = rel.getConvention();
        if (!convention._interface.isInstance(rel) &&
                !(rel instanceof ConverterRel)) {
            throw Util.newInternal("Relational expression " + rel +
                    " has calling-convention " + convention +
                    " but does not implement the required interface '" +
                    convention._interface + "' of that convention");
        }

        // Ensure that its sub-expressions are registered.
        rel.onRegister(this);

        // If it is equivalent to an existing expression, return the set that
        // the equivalent expression belongs to.
        String digest = rel.toString();
        SaffronRel equivExp = (SaffronRel) mapDigestToRel.get(digest);
        if (equivExp == null) {
            ;
        } else if (equivExp == rel) {
            return getSubset(rel);
        } else {
            assert(
                (equivExp.getConvention() == convention)
                    && (equivExp.getClass() == rel.getClass()));
            RelSet equivSet = getSet(equivExp);
            if (equivSet != null) {
                tracer.finer("Register: rel #" + rel.getId() +
                        " is equivalent to rel #" + equivExp);
                return registerSubset(set,getSubset(equivExp));
            }
        }

        // Converters are in the same set as their children.
        if (rel instanceof ConverterRel) {
            final SaffronRel input = ((ConverterRel) rel).child;
            final RelSet childSet = getSet(input);
            if (
                (set != null)
                    && (set != childSet)
                    && (set.equivalentSet == null)) {
                tracer.finer(
                    "Register #" + rel.getId() + " " + digest
                    + " (and merge sets, because it is a conversion)");
                merge(set,childSet);
                registerCount++;

                // During the mergers, the child set may have changed, and since
                // we're not registered yet, we won't have been informed. So
                // check whether we are now equivalent to an existing
                // expression.
                if (fixupInputs(rel)) {
                    digest = rel.recomputeDigest();
                    SaffronRel equivRel =
                        (SaffronRel) mapDigestToRel.get(digest);
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
                    OptUtil.getVariablesSet(rel),
                    rel.getVariablesStopped());
            set.variablesUsed = OptUtil.getVariablesUsed(rel);
            this.allSets.add(set);
        }

        // Chain to find 'live' equivalent set, just in case several sets are
        // merging at the same time.
        while (set.equivalentSet != null) {
            set = set.equivalentSet;
        }
        registerCount++;
        RelSubset subset = set.add(rel);
        mapRel2Subset.put(rel,subset);
        final Object xx = mapDigestToRel.put(digest,rel);
        assert((xx == null) || (xx == rel));
        tracer.finer(
            "Register #" + rel.getId() + " " + digest + " in " + subset);

        // This relational expression may have been registered while we
        // recursively registered its children. If this is the case, we're done.
        if (xx != null) {
            return subset;
        }

        // Create back-links from its children, which makes children more
        // important.
        if (rel == this.root) {
            ruleQueue.subsetImportances.put(subset,new Double(1.0)); // todo: remove
        }
        SaffronRel [] inputs = rel.getInputs();
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
        checkForSatisfiedConverters(set,rel);

        // Add relational expression to queue of expressions whose rules
        // have not been fired.
        ruleQueue.add(rel);

        // Queue up all rules triggered by this relexp's creation.
        fireRules(rel,true);

        return subset;
    }

    private RelSubset registerSubset(RelSet set,RelSubset subset)
    {
        if (
            (set != subset.set)
                && (set != null)
                && (set.equivalentSet == null)) {
            tracer.finer(
                "Register #" + subset.getId() + " " + subset
                + ", and merge sets");
            merge(set,subset.set);
            registerCount++;
        }
        return subset;
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * A rule call which defers its actions. Whereas {@link VolcanoRuleCall}
     * invokes the rule when it finds a match, a
     * <code>DeferringRuleCall</code> creates a {@link VolcanoRuleMatch}
     * which can be invoked later.
     */
    private static class DeferringRuleCall extends VolcanoRuleCall
    {
        DeferringRuleCall(VolcanoPlanner planner, RuleOperand operand)
        {
            super(planner, operand);
        }

        /**
         * Rather than invoking the rule (as the base method does), creates a
         * {@link VolcanoRuleMatch} which can be invoked later.
         */
        protected void onMatch()
        {
            final VolcanoRuleMatch match = new VolcanoRuleMatch(planner, operand0,rels);
            planner.ruleQueue.addMatch(match);
        }
    }
}


// End VolcanoPlanner.java
