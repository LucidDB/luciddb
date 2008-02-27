/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 The Eigenbase Project
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

import java.util.*;

import junit.framework.*;

import openjava.mop.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * A <code>VolcanoPlannerTest</code> is a unit-test for {@link VolcanoPlanner
 * the optimizer}.
 *
 * @author John V. Sichi
 * @version $Id$
 * @since Mar 19, 2003
 */
public class VolcanoPlannerTest
    extends TestCase
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Private calling convention representing a physical implementation.
     */
    private static final CallingConvention PHYS_CALLING_CONVENTION =
        new CallingConvention(
            "PHYS",
            CallingConvention.generateOrdinal(),
            RelNode.class);

    //~ Constructors -----------------------------------------------------------

    public VolcanoPlannerTest(String name)
    {
        super(name);
    }

    //~ Methods ----------------------------------------------------------------

    static RelOptCluster newCluster(VolcanoPlanner planner)
    {
        RelOptQuery query = new RelOptQuery(planner);
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl();
        return query.createCluster(
            new TestEnvironment(),
            typeFactory,
            new RexBuilder(typeFactory));
    }

    /**
     * Tests transformation of a leaf from NONE to PHYS.
     */
    public void testTransformLeaf()
    {
        VolcanoPlanner planner = new VolcanoPlanner();

        planner.addRelTraitDef(CallingConventionTraitDef.instance);

        planner.addRule(new PhysLeafRule());

        NoneLeafRel leafRel =
            new NoneLeafRel(
                newCluster(planner),
                "a");
        RelNode convertedRel =
            planner.changeTraits(
                leafRel,
                new RelTraitSet(PHYS_CALLING_CONVENTION));
        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysLeafRel);
    }

    /**
     * Tests transformation of a single+leaf from NONE to PHYS.
     */
    public void testTransformSingleGood()
    {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(CallingConventionTraitDef.instance);

        planner.addRule(new PhysLeafRule());
        planner.addRule(new GoodSingleRule());

        NoneLeafRel leafRel =
            new NoneLeafRel(
                newCluster(planner),
                "a");
        NoneSingleRel singleRel =
            new NoneSingleRel(
                leafRel.getCluster(),
                leafRel);
        RelNode convertedRel =
            planner.changeTraits(
                singleRel,
                new RelTraitSet(PHYS_CALLING_CONVENTION));
        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysSingleRel);
    }

    /**
     * Tests transformation of a single+leaf from NONE to PHYS. In the past,
     * this one didn't work due to the definition of ReformedSingleRule.
     */
    public void testTransformSingleReformed()
    {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(CallingConventionTraitDef.instance);

        planner.addRule(new PhysLeafRule());
        planner.addRule(new ReformedSingleRule());

        NoneLeafRel leafRel =
            new NoneLeafRel(
                newCluster(planner),
                "a");
        NoneSingleRel singleRel =
            new NoneSingleRel(
                leafRel.getCluster(),
                leafRel);
        RelNode convertedRel =
            planner.changeTraits(
                singleRel,
                new RelTraitSet(PHYS_CALLING_CONVENTION));
        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysSingleRel);
    }

    private void removeTrivialProject(boolean useRule)
    {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.ambitious = true;

        planner.addRelTraitDef(CallingConventionTraitDef.instance);

        if (useRule) {
            planner.addRule(RemoveTrivialProjectRule.instance);
        }

        planner.addRule(new PhysLeafRule());
        planner.addRule(new GoodSingleRule());
        planner.addRule(new PhysProjectRule());

        planner.addRule(
            new ConverterRule(
                RelNode.class,
                PHYS_CALLING_CONVENTION,
                CallingConvention.ITERATOR,
                "PhysToIteratorRule") {
                public RelNode convert(RelNode rel)
                {
                    return new PhysToIteratorConverter(
                        rel.getCluster(),
                        rel);
                }
            });

        PhysLeafRel leafRel =
            new PhysLeafRel(
                newCluster(planner),
                "a");
        RexInputRef inputRef =
            new RexInputRef(
                0,
                leafRel.getRowType().getFields()[0].getType());
        RelNode projectRel =
            CalcRel.createProject(
                leafRel,
                new RexNode[] { inputRef },
                new String[] { "this" });
        NoneSingleRel singleRel =
            new NoneSingleRel(
                projectRel.getCluster(),
                projectRel);
        RelNode convertedRel =
            planner.changeTraits(
                singleRel,
                new RelTraitSet(CallingConvention.ITERATOR));
        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysToIteratorConverter);
    }

    // NOTE:  this used to fail but now works
    public void testWithRemoveTrivialProject()
    {
        removeTrivialProject(true);
    }

    // NOTE:  this always worked; it's here as constrast to
    // testWithRemoveTrivialProject()
    public void testWithoutRemoveTrivialProject()
    {
        removeTrivialProject(false);
    }

    /**
     * Previously, this didn't work because ReformedRemoveSingleRule uses a
     * pattern which spans calling conventions.
     */
    public void testRemoveSingleReformed()
    {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.ambitious = true;
        planner.addRelTraitDef(CallingConventionTraitDef.instance);

        planner.addRule(new PhysLeafRule());
        planner.addRule(new ReformedRemoveSingleRule());

        NoneLeafRel leafRel =
            new NoneLeafRel(
                newCluster(planner),
                "a");
        NoneSingleRel singleRel =
            new NoneSingleRel(
                leafRel.getCluster(),
                leafRel);
        RelNode convertedRel =
            planner.changeTraits(
                singleRel,
                new RelTraitSet(PHYS_CALLING_CONVENTION));
        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysLeafRel);
        PhysLeafRel resultLeaf = (PhysLeafRel) result;
        assertEquals(
            "c",
            resultLeaf.getLabel());
    }

    /**
     * This always worked (in contrast to testRemoveSingleReformed) because it
     * uses a completely-physical pattern (requiring GoodSingleRule to fire
     * first).
     */
    public void testRemoveSingleGood()
    {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.ambitious = true;
        planner.addRelTraitDef(CallingConventionTraitDef.instance);

        planner.addRule(new PhysLeafRule());
        planner.addRule(new GoodSingleRule());
        planner.addRule(new GoodRemoveSingleRule());

        NoneLeafRel leafRel =
            new NoneLeafRel(
                newCluster(planner),
                "a");
        NoneSingleRel singleRel =
            new NoneSingleRel(
                leafRel.getCluster(),
                leafRel);
        RelNode convertedRel =
            planner.changeTraits(
                singleRel,
                new RelTraitSet(PHYS_CALLING_CONVENTION));
        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysLeafRel);
        PhysLeafRel resultLeaf = (PhysLeafRel) result;
        assertEquals(
            "c",
            resultLeaf.getLabel());
    }

    /**
     * Tests whether planner correctly notifies listeners of events.
     */
    public void testListener()
    {
        TestListener listener = new TestListener();

        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addListener(listener);

        planner.addRelTraitDef(CallingConventionTraitDef.instance);

        planner.addRule(new PhysLeafRule());

        NoneLeafRel leafRel =
            new NoneLeafRel(
                newCluster(planner),
                "a");
        RelNode convertedRel =
            planner.changeTraits(
                leafRel,
                new RelTraitSet(PHYS_CALLING_CONVENTION));
        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysLeafRel);

        List<RelOptListener.RelEvent> eventList = listener.getEventList();

        // add node
        checkEvent(
            eventList,
            0,
            RelOptListener.RelEquivalenceEvent.class,
            leafRel,
            null);

        // internal subset
        checkEvent(
            eventList,
            1,
            RelOptListener.RelEquivalenceEvent.class,
            null,
            null);

        // before rule
        checkEvent(
            eventList,
            2,
            RelOptListener.RuleAttemptedEvent.class,
            leafRel,
            PhysLeafRule.class);

        // before rule
        checkEvent(
            eventList,
            3,
            RelOptListener.RuleProductionEvent.class,
            result,
            PhysLeafRule.class);

        // result of rule
        checkEvent(
            eventList,
            4,
            RelOptListener.RelEquivalenceEvent.class,
            result,
            null);

        // after rule
        checkEvent(
            eventList,
            5,
            RelOptListener.RuleProductionEvent.class,
            result,
            PhysLeafRule.class);

        // after rule
        checkEvent(
            eventList,
            6,
            RelOptListener.RuleAttemptedEvent.class,
            leafRel,
            PhysLeafRule.class);

        // choose plan
        checkEvent(
            eventList,
            7,
            RelOptListener.RelChosenEvent.class,
            result,
            null);

        // finish choosing plan
        checkEvent(
            eventList,
            8,
            RelOptListener.RelChosenEvent.class,
            null,
            null);
    }

    private void checkEvent(
        List<RelOptListener.RelEvent> eventList,
        int iEvent,
        Class expectedEventClass,
        RelNode expectedRel,
        Class<? extends RelOptRule> expectedRuleClass)
    {
        assertTrue(iEvent < eventList.size());
        RelOptListener.RelEvent event = eventList.get(iEvent);
        assertSame(
            expectedEventClass,
            event.getClass());
        if (expectedRel != null) {
            assertSame(
                expectedRel,
                event.getRel());
        }
        if (expectedRuleClass != null) {
            RelOptListener.RuleEvent ruleEvent =
                (RelOptListener.RuleEvent) event;
            assertSame(
                expectedRuleClass,
                ruleEvent.getRuleCall().getRule().getClass());
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class TestEnvironment
        extends GlobalEnvironment
    {
        public String toString()
        {
            return null;
        }

        public void record(
            String name,
            OJClass clazz)
        {
            throw new AssertionError();
        }
    }

    private static abstract class TestLeafRel
        extends AbstractRelNode
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
        public TestLeafRel clone()
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
            return getCluster().getTypeFactory().createStructType(
                new RelDataType[] {
                    getCluster().getTypeFactory().createJavaType(Void.TYPE)
                },
                new String[] { "this" });
        }

        public void explain(RelOptPlanWriter pw)
        {
            pw.explain(
                this,
                new String[] { "label" },
                new Object[] { label });
        }
    }

    private static abstract class TestSingleRel
        extends SingleRel
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
            return getChild().getRowType();
        }
    }

    private static class NoneSingleRel
        extends TestSingleRel
    {
        protected NoneSingleRel(
            RelOptCluster cluster,
            RelNode child)
        {
            super(
                cluster,
                new RelTraitSet(CallingConvention.NONE),
                child);
        }

        // implement RelNode
        public NoneSingleRel clone()
        {
            NoneSingleRel clone =
                new NoneSingleRel(
                    getCluster(),
                    getChild());
            clone.inheritTraitsFrom(this);
            return clone;
        }
    }

    private static class NoneLeafRel
        extends TestLeafRel
    {
        protected NoneLeafRel(
            RelOptCluster cluster,
            String label)
        {
            super(
                cluster,
                new RelTraitSet(CallingConvention.NONE),
                label);
        }
    }

    private static class PhysLeafRel
        extends TestLeafRel
    {
        PhysLeafRel(
            RelOptCluster cluster,
            String label)
        {
            super(
                cluster,
                new RelTraitSet(PHYS_CALLING_CONVENTION),
                label);
        }

        // implement RelNode
        public RelOptCost computeSelfCost(RelOptPlanner planner)
        {
            return planner.makeTinyCost();
        }
    }

    private static class PhysSingleRel
        extends TestSingleRel
    {
        PhysSingleRel(
            RelOptCluster cluster,
            RelNode child)
        {
            super(
                cluster,
                new RelTraitSet(PHYS_CALLING_CONVENTION),
                child);
        }

        // implement RelNode
        public RelOptCost computeSelfCost(RelOptPlanner planner)
        {
            return planner.makeTinyCost();
        }

        // implement RelNode
        public PhysSingleRel clone()
        {
            PhysSingleRel clone =
                new PhysSingleRel(
                    getCluster(),
                    getChild());
            clone.inheritTraitsFrom(this);
            return clone;
        }
    }

    class PhysToIteratorConverter
        extends ConverterRelImpl
    {
        public PhysToIteratorConverter(
            RelOptCluster cluster,
            RelNode child)
        {
            super(
                cluster,
                CallingConventionTraitDef.instance,
                new RelTraitSet(CallingConvention.ITERATOR),
                child);
        }

        // implement RelNode
        public PhysToIteratorConverter clone()
        {
            PhysToIteratorConverter clone =
                new PhysToIteratorConverter(
                    getCluster(),
                    getChild());
            clone.inheritTraitsFrom(this);
            return clone;
        }
    }

    private static class PhysLeafRule
        extends RelOptRule
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

    private static class GoodSingleRule
        extends RelOptRule
    {
        GoodSingleRule()
        {
            super(new RelOptRuleOperand(
                    NoneSingleRel.class,
                    null));
        }

        // implement RelOptRule
        public CallingConvention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            NoneSingleRel singleRel = (NoneSingleRel) call.rels[0];
            RelNode childRel = singleRel.getChild();
            RelNode physInput =
                mergeTraitsAndConvert(
                    singleRel.getTraits(),
                    PHYS_CALLING_CONVENTION,
                    childRel);
            call.transformTo(
                new PhysSingleRel(
                    singleRel.getCluster(),
                    physInput));
        }
    }

    // NOTE: Previously, ReformedSingleRule did't work because it explicitly
    // specifies PhysLeafRel rather than RelNode for the single input.  Since
    // the PhysLeafRel is in a different subset from the original NoneLeafRel,
    // ReformedSingleRule never saw it.  (GoodSingleRule saw the NoneLeafRel
    // instead and fires off of that; later the NoneLeafRel gets converted into
    // a PhysLeafRel).  Now Volcano supports rules which match across subsets.
    private static class ReformedSingleRule
        extends RelOptRule
    {
        ReformedSingleRule()
        {
            super(
                new RelOptRuleOperand(
                    NoneSingleRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(PhysLeafRel.class, null)
                    }));
        }

        // implement RelOptRule
        public CallingConvention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            NoneSingleRel singleRel = (NoneSingleRel) call.rels[0];
            RelNode childRel = call.rels[1];
            RelNode physInput =
                mergeTraitsAndConvert(
                    singleRel.getTraits(),
                    PHYS_CALLING_CONVENTION,
                    childRel);
            call.transformTo(
                new PhysSingleRel(
                    singleRel.getCluster(),
                    physInput));
        }
    }

    private static class PhysProjectRule
        extends RelOptRule
    {
        PhysProjectRule()
        {
            super(new RelOptRuleOperand(
                    ProjectRel.class,
                    null));
        }

        // implement RelOptRule
        public CallingConvention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            RelNode childRel = ((ProjectRel) call.rels[0]).getChild();
            call.transformTo(new PhysLeafRel(
                    childRel.getCluster(),
                    "b"));
        }
    }

    private static class GoodRemoveSingleRule
        extends RelOptRule
    {
        GoodRemoveSingleRule()
        {
            super(
                new RelOptRuleOperand(
                    PhysSingleRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(PhysLeafRel.class, null)
                    }));
        }

        // implement RelOptRule
        public CallingConvention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            PhysSingleRel singleRel = (PhysSingleRel) call.rels[0];
            PhysLeafRel leafRel = (PhysLeafRel) call.rels[1];
            call.transformTo(new PhysLeafRel(
                    singleRel.getCluster(),
                    "c"));
        }
    }

    private static class ReformedRemoveSingleRule
        extends RelOptRule
    {
        ReformedRemoveSingleRule()
        {
            super(
                new RelOptRuleOperand(
                    NoneSingleRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(PhysLeafRel.class, null)
                    }));
        }

        // implement RelOptRule
        public CallingConvention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            NoneSingleRel singleRel = (NoneSingleRel) call.rels[0];
            PhysLeafRel leafRel = (PhysLeafRel) call.rels[1];
            call.transformTo(new PhysLeafRel(
                    singleRel.getCluster(),
                    "c"));
        }
    }

    private static class TestListener
        implements RelOptListener
    {
        private List<RelEvent> eventList;

        TestListener()
        {
            eventList = new ArrayList<RelEvent>();
        }

        List<RelEvent> getEventList()
        {
            return eventList;
        }

        private void recordEvent(RelEvent event)
        {
            eventList.add(event);
        }

        // implement RelOptListener
        public void relChosen(RelChosenEvent event)
        {
            recordEvent(event);
        }

        // implement RelOptListener
        public void relDiscarded(RelDiscardedEvent event)
        {
            // Volcano is quite a packrat--it never discards anything!
            throw Util.newInternal(event.toString());
        }

        // implement RelOptListener
        public void relEquivalenceFound(RelEquivalenceEvent event)
        {
            if (!event.isPhysical()) {
                return;
            }
            recordEvent(event);
        }

        // implement RelOptListener
        public void ruleAttempted(RuleAttemptedEvent event)
        {
            recordEvent(event);
        }

        // implement RelOptListener
        public void ruleProductionSucceeded(RuleProductionEvent event)
        {
            recordEvent(event);
        }
    }
}

// End VolcanoPlannerTest.java
