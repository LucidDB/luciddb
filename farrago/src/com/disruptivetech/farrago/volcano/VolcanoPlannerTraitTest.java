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

import junit.framework.TestCase;
import org.eigenbase.relopt.*;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.AbstractRelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.rel.convert.ConverterRel;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.util.MultiMap;
import openjava.ptree.ParseTree;

import java.util.List;
import java.util.Iterator;

/**
 * VolcanoPlannerTraitTest
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class VolcanoPlannerTraitTest
    extends TestCase
{
    //~ Static fields/initializers --------------------------------------------

    /**
     * Private calling convention representing a generic "physical" calling 
     * convention.
     */
    private static final CallingConvention PHYS_CALLING_CONVENTION =
        new CallingConvention("PHYS",
            CallingConvention.generateOrdinal(), RelNode.class);

    /**
     * Private trait definition for an alternate type of traits. 
     */ 
    private static final AltTraitDef ALT_TRAIT_DEF =
        new AltTraitDef();
    
    /**
     * Private alternate trait.
     */ 
    private static final AltTrait ALT_TRAIT =
        new AltTrait(ALT_TRAIT_DEF, "ALT");

    /**
     * Private alternate trait.
     */
    private static final AltTrait ALT_TRAIT2 =
        new AltTrait(ALT_TRAIT_DEF, "ALT2");

    /**
     * Ordinal count for alternate traits (so they can implement equals()
     * and avoid being canonized into the same trait).
     */
    private static int altTraitOrdinal = 0;


    //~ Constructors ----------------------------------------------------------

    public VolcanoPlannerTraitTest(String name)
    {
        super(name);
    }

    //~ Methods ---------------------------------------------------------------

    public void testDoubleConversion()
    {
        VolcanoPlanner planner = new VolcanoPlanner();

        planner.addRelTraitDef(CallingConventionTraitDef.instance);
        planner.addRelTraitDef(ALT_TRAIT_DEF);

        planner.addRule(new PhysToIteratorConverterRule());
        planner.addRule(
            new AltTraitConverterRule(
                ALT_TRAIT, ALT_TRAIT2, "AltToAlt2ConverterRule"
            ));
        planner.addRule(new PhysLeafRule());
        planner.addRule(new IterSingleRule());

        RelOptCluster cluster = VolcanoPlannerTest.newCluster(planner);

        NoneLeafRel noneLeafRel = new NoneLeafRel(cluster, "noneLeafRel");
        noneLeafRel.getTraits().addTrait(ALT_TRAIT);

        NoneSingleRel noneRel = new NoneSingleRel(cluster, noneLeafRel);
        noneRel.getTraits().addTrait(ALT_TRAIT2);

        RelNode convertedRel =
            planner.changeTraits(
                noneRel,
                new RelTraitSet(
                    new RelTrait[] {
                        CallingConvention.ITERATOR, ALT_TRAIT2 }));

        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();

        assertTrue(result instanceof IterSingleRel);
        assertEquals(
            CallingConvention.ITERATOR,
            result.getTraits().getTrait(CallingConventionTraitDef.instance));
        assertEquals(ALT_TRAIT2, result.getTraits().getTrait(ALT_TRAIT_DEF));

        RelNode child = result.getInput(0);
        assertTrue(
            child instanceof AltTraitConverter ||
            child instanceof PhysToIteratorConverter);

        child = child.getInput(0);
        assertTrue(
            child instanceof AltTraitConverter ||
            child instanceof PhysToIteratorConverter);

        child = child.getInput(0);
        assertTrue(child instanceof PhysLeafRel);
    }


    public void testTraitPropagation()
    {
        VolcanoPlanner planner = new VolcanoPlanner();

        planner.addRelTraitDef(CallingConventionTraitDef.instance);
        planner.addRelTraitDef(ALT_TRAIT_DEF);

        planner.addRule(new PhysToIteratorConverterRule());
        planner.addRule(
            new AltTraitConverterRule(
                ALT_TRAIT, ALT_TRAIT2, "AltToAlt2ConverterRule"
            ));
        planner.addRule(new PhysLeafRule());
        planner.addRule(new IterSingleRule2());

        RelOptCluster cluster = VolcanoPlannerTest.newCluster(planner);

        NoneLeafRel noneLeafRel = new NoneLeafRel(cluster, "noneLeafRel");
        noneLeafRel.getTraits().addTrait(ALT_TRAIT);

        NoneSingleRel noneRel = new NoneSingleRel(cluster, noneLeafRel);
        noneRel.getTraits().addTrait(ALT_TRAIT2);

        RelNode convertedRel =
            planner.changeTraits(
                noneRel,
                new RelTraitSet(
                    new RelTrait[] {
                        CallingConvention.ITERATOR, ALT_TRAIT2 }));

        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();

        assertTrue(result instanceof IterSingleRel);
        assertEquals(
            CallingConvention.ITERATOR,
            result.getTraits().getTrait(CallingConventionTraitDef.instance));
        assertEquals(ALT_TRAIT2, result.getTraits().getTrait(ALT_TRAIT_DEF));

        RelNode child = result.getInput(0);
        assertTrue(child instanceof IterSingleRel);
        assertEquals(
            CallingConvention.ITERATOR,
            child.getTraits().getTrait(CallingConventionTraitDef.instance));
        assertEquals(ALT_TRAIT2, child.getTraits().getTrait(ALT_TRAIT_DEF));

        child = child.getInput(0);
        assertTrue(
            child instanceof AltTraitConverter ||
            child instanceof PhysToIteratorConverter);

        child = child.getInput(0);
        assertTrue(
            child instanceof AltTraitConverter ||
            child instanceof PhysToIteratorConverter);

        child = child.getInput(0);
        assertTrue(child instanceof PhysLeafRel);
    }


    //~ Inner Classes ---------------------------------------------------------

    private static class AltTrait
        implements RelTrait
    {
        private final AltTraitDef traitDef;
        private final int ordinal;
        private final String description;

        private AltTrait(AltTraitDef traitDef, String description)
        {
            this.traitDef = traitDef;
            this.description = description;
            this.ordinal = altTraitOrdinal++;
        }

        public RelTraitDef getTraitDef()
        {
            return traitDef;
        }

        public boolean equals(Object other)
        {
            if (other == null) {
                return false;
            }

            AltTrait that = (AltTrait)other;
            return this.ordinal == that.ordinal;
        }

        public int hashCode()
        {
            return ordinal;
        }

        public String toString()
        {
            return description;
        }
    }

    private static class AltTraitDef
        extends RelTraitDef
    {
        private MultiMap conversionMap = new MultiMap();

        public Class getTraitClass()
        {
            return AltTrait.class;
        }

        public String getSimpleName()
        {
            return "alt_phys";
        }

        public RelNode convert(
            RelOptPlanner planner, RelNode rel, RelTrait toTrait, 
            boolean allowInfiniteCostConverters)
        {
            RelTrait fromTrait = rel.getTraits().getTrait(this);

            if (conversionMap.containsKey(fromTrait)) {
                List entries = conversionMap.getMulti(fromTrait);
                for(Iterator i = entries.iterator(); i.hasNext(); ) {
                    Object[] traitAndRule = (Object[])i.next();

                    RelTrait trait = (RelTrait)traitAndRule[0];
                    ConverterRule rule = (ConverterRule)traitAndRule[1];

                    if (trait == toTrait) {
                        RelNode converted = rule.convert(rel);
                        if (converted != null &&
                            (!planner.getCost(converted).isInfinite() ||
                                allowInfiniteCostConverters)) {
                            return converted;
                        }
                    }
                }
            }

            return null;
        }

        public boolean canConvert(
            RelOptPlanner planner, RelTrait fromTrait, RelTrait toTrait)
        {
            if (conversionMap.containsKey(fromTrait)) {
                List entries = conversionMap.getMulti(fromTrait);
                for(Iterator i = entries.iterator(); i.hasNext(); ) {
                    Object[] traitAndRule = (Object[])i.next();

                    if (traitAndRule[0] == toTrait) {
                        return true;
                    }
                }
            }

            return false;
        }

        public void registerConverterRule(
            RelOptPlanner planner, ConverterRule converterRule)
        {
            if (!converterRule.isGuaranteed()) {
                return;
            }

            RelTrait fromTrait = converterRule.getInTraits().getTrait(this);
            RelTrait toTrait = converterRule.getOutTraits().getTrait(this);

            conversionMap.putMulti(
                fromTrait, new Object[] { toTrait, converterRule });
        }
    }

    private static abstract class TestLeafRel extends AbstractRelNode
    {
        private String label;

        protected TestLeafRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            String label)
        {
            super(cluster, traits);
            this.label = label;
        }

        public String getLabel()
        {
            return label;
        }

        // implement RelNode
        public Object clone()
        {
            return this;
        }

        // implement RelNode
        public RelOptCost computeSelfCost(RelOptPlanner planner)
        {
            return planner.makeInfiniteCost();
        }

        // implement RelNode
        protected RelDataType deriveRowType()
        {
            return cluster.typeFactory.createStructType(
                new RelDataType [] {
                    cluster.typeFactory.createJavaType(Void.TYPE)
                },
                new String [] { "this" });
        }

        public void explain(RelOptPlanWriter pw)
        {
            pw.explain(
                this,
                new String [] { "label" },
                new Object [] { label });
        }
    }

    private static class NoneLeafRel extends TestLeafRel
    {
        protected NoneLeafRel(
            RelOptCluster cluster,
            String label)
        {
            super(cluster, new RelTraitSet(CallingConvention.NONE), label);
        }
    }

    private static class PhysLeafRel extends TestLeafRel
    {
        PhysLeafRel(
            RelOptCluster cluster,
            String label)
        {
            super(cluster, new RelTraitSet(PHYS_CALLING_CONVENTION), label);
        }

        // implement RelNode
        public RelOptCost computeSelfCost(RelOptPlanner planner)
        {
            return planner.makeTinyCost();
        }

        // TODO: SWZ Implement clone?
    }

    private static abstract class TestSingleRel extends SingleRel
    {
        protected TestSingleRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child)
        {
            super(cluster, traits, child);
        }

        // implement RelNode
        public RelOptCost computeSelfCost(RelOptPlanner planner)
        {
            return planner.makeInfiniteCost();
        }

        // implement RelNode
        protected RelDataType deriveRowType()
        {
            return child.getRowType();
        }

        // TODO: SWZ Implement clone?
    }

    private static class NoneSingleRel extends TestSingleRel
    {
        protected NoneSingleRel(
            RelOptCluster cluster,
            RelNode child)
        {
            super(cluster, new RelTraitSet(CallingConvention.NONE), child);
        }

        // implement RelNode
        public Object clone()
        {
            NoneSingleRel clone = new NoneSingleRel(cluster, child);
            clone.inheritTraitsFrom(this);
            return clone;
        }
    }


    private static class IterSingleRel extends TestSingleRel implements JavaRel
    {
        public IterSingleRel(RelOptCluster cluster, RelNode child)
        {
            super(cluster, new RelTraitSet(CallingConvention.ITERATOR), child);
        }

        // implement RelNode
        public RelOptCost computeSelfCost(RelOptPlanner planner)
        {
            return planner.makeTinyCost();
        }

        public Object clone()
        {
            IterSingleRel clone = new IterSingleRel(getCluster(), getInput(0));
            clone.inheritTraitsFrom(this);
            return clone;
        }

        public ParseTree implement(JavaRelImplementor implementor)
        {
            return null;
        }
    }

    private static class PhysLeafRule extends RelOptRule
    {
        PhysLeafRule()
        {
            super(new RelOptRuleOperand(NoneLeafRel.class, null));
        }

        // implement RelOptRule
        public CallingConvention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            NoneLeafRel leafRel = (NoneLeafRel) call.rels[0];
            call.transformTo(
                new PhysLeafRel(
                    leafRel.getCluster(),
                    leafRel.getLabel()));
        }
    }


    private static class IterSingleRule extends RelOptRule
    {
        IterSingleRule()
        {
            super(new RelOptRuleOperand(NoneSingleRel.class, null));
        }

        // implement RelOptRule
        public CallingConvention getOutConvention()
        {
            return CallingConvention.ITERATOR;
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            NoneSingleRel rel = (NoneSingleRel) call.rels[0];

            RelNode converted =
                mergeTraitsAndConvert(
                    rel.getTraits(), getOutTraits(), rel.getInput(0));

            call.transformTo(
                new IterSingleRel(
                    rel.getCluster(),
                    converted));
        }
    }


    private static class IterSingleRule2 extends RelOptRule
    {
        IterSingleRule2()
        {
            super(new RelOptRuleOperand(NoneSingleRel.class, null));
        }

        // implement RelOptRule
        public CallingConvention getOutConvention()
        {
            return CallingConvention.ITERATOR;
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            NoneSingleRel rel = (NoneSingleRel) call.rels[0];

            RelNode converted =
                mergeTraitsAndConvert(
                    rel.getTraits(), getOutTraits(), rel.getInput(0));

            IterSingleRel child =
                new IterSingleRel(rel.getCluster(), converted);

            call.transformTo(
                new IterSingleRel(
                    rel.getCluster(),
                    child));
        }
    }


    private static class AltTraitConverterRule extends ConverterRule
    {
        private final RelTrait toTrait;

        private AltTraitConverterRule(
            AltTrait fromTrait, AltTrait toTrait, String description)
        {
            super(
                RelNode.class,
                new RelTraitSet(new RelTrait[] { null, fromTrait }),
                new RelTraitSet(new RelTrait[] { null, toTrait } ),
                description);

            this.toTrait = toTrait;
        }

        public RelNode convert(RelNode rel)
        {
            return new AltTraitConverter(rel.getCluster(), rel, toTrait);
        }

        public boolean isGuaranteed()
        {
            return true;
        }
    }

    private static class AltTraitConverter extends ConverterRel
    {
        private final RelTrait toTrait;

        private AltTraitConverter(
            RelOptCluster cluster, RelNode child, RelTrait toTrait)
        {
            super(
                cluster, toTrait.getTraitDef(),
                convertTraits(child.getTraits(), toTrait), child);

            this.toTrait = toTrait;
        }

        // override Object (public, does not throw CloneNotSupportedException)
        public Object clone()
        {
            AltTraitConverter clone =
                new AltTraitConverter(getCluster(), getInput(0), toTrait);
            clone.inheritTraitsFrom(this);
            return clone;
        }
    }

    private static class PhysToIteratorConverterRule extends ConverterRule
    {
        public PhysToIteratorConverterRule()
        {
            super(
                RelNode.class, PHYS_CALLING_CONVENTION,
                CallingConvention.ITERATOR, "PhysToIteratorRule");
        }

        public RelNode convert(RelNode rel)
        {
            return new PhysToIteratorConverter(
                rel.getCluster(),
                rel);
        }
    }

    private static class PhysToIteratorConverter extends ConverterRel
    {
        public PhysToIteratorConverter(
            RelOptCluster cluster,
            RelNode child)
        {
            super(
                cluster,
                CallingConventionTraitDef.instance,
                convertTraits(
                    child.getTraits(), CallingConvention.ITERATOR),
                child);
        }

        // implement RelNode
        public Object clone()
        {
            PhysToIteratorConverter clone =
                new PhysToIteratorConverter(cluster, child);
            clone.inheritTraitsFrom(this);
            return clone;
        }
    }
}
