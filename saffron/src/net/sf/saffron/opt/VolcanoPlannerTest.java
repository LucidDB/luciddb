/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

import junit.framework.TestCase;
import net.sf.saffron.core.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rel.convert.*;
import net.sf.saffron.rex.*;
import openjava.mop.*;

/**
 * A <code>VolcanoPlannerTest</code> is a unit-test for {@link VolcanoPlanner
 * the optimizer}.
 *
 * @author John V. Sichi
 * @version $Id$
 * @since Mar 19, 2003
 */
public class VolcanoPlannerTest extends TestCase
{
    /**
     * Private calling convention representing a physical implementation.
     */
    private static final CallingConvention PHYS_CALLING_CONVENTION =
        new CallingConvention(
            "PHYS",
            CallingConvention.enumeration.getMax() + 1);

    public VolcanoPlannerTest(String name)
    {
        super(name);
    }

    private VolcanoCluster newCluster(VolcanoPlanner planner)
    {
        VolcanoQuery query = new VolcanoQuery(planner);
        SaffronTypeFactory typeFactory = new SaffronTypeFactoryImpl();
        return query.createCluster(
            new TestEnvironment(),
            typeFactory,
            new RexBuilder(typeFactory));
    }

    /**
     * Test transformation of a leaf from NONE to PHYS.
     */
    public void testTransformLeaf()
    {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addCallingConvention(CallingConvention.NONE);
        planner.addCallingConvention(PHYS_CALLING_CONVENTION);

        planner.addRule(new PhysLeafRule());

        NoneLeafRel leafRel = new NoneLeafRel(newCluster(planner),"a");
        SaffronRel convertedRel = planner.changeConvention(
            leafRel,PHYS_CALLING_CONVENTION);
        planner.setRoot(convertedRel);
        SaffronRel result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysLeafRel);
    }

    /**
     * Test transformation of a single+leaf from NONE to PHYS.
     */
    public void testTransformSingleGood()
    {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addCallingConvention(CallingConvention.NONE);
        planner.addCallingConvention(PHYS_CALLING_CONVENTION);

        planner.addRule(new PhysLeafRule());
        planner.addRule(new GoodSingleRule());
        
        NoneLeafRel leafRel = new NoneLeafRel(newCluster(planner),"a");
        NoneSingleRel singleRel = new NoneSingleRel(
            leafRel.getCluster(),leafRel);
        SaffronRel convertedRel = planner.changeConvention(
            singleRel,PHYS_CALLING_CONVENTION);
        planner.setRoot(convertedRel);
        SaffronRel result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysSingleRel);
    }

    /**
     * Test transformation of a single+leaf from NONE to PHYS.
     * This one doesn't work due to the definition of BadSingleRule.
     */
    public void testTransformSingleBad()
    {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addCallingConvention(CallingConvention.NONE);
        planner.addCallingConvention(PHYS_CALLING_CONVENTION);

        planner.addRule(new PhysLeafRule());
        planner.addRule(new BadSingleRule());

        NoneLeafRel leafRel = new NoneLeafRel(newCluster(planner),"a");
        NoneSingleRel singleRel = new NoneSingleRel(
            leafRel.getCluster(),leafRel);
        SaffronRel convertedRel = planner.changeConvention(
            singleRel,PHYS_CALLING_CONVENTION);
        planner.setRoot(convertedRel);
        SaffronRel result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysSingleRel);
    }

    private void removeTrivialProject(boolean useRule)
    {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addCallingConvention(CallingConvention.NONE);
        planner.addCallingConvention(CallingConvention.ITERATOR);
        planner.addCallingConvention(PHYS_CALLING_CONVENTION);

        if (useRule) {
            planner.addRule(new RemoveTrivialProjectRule());
        }
        
        planner.addRule(new PhysLeafRule());
        planner.addRule(new GoodSingleRule());
        planner.addRule(new PhysProjectRule());

        planner.addRule(
            new ConverterRule(
                SaffronRel.class,
                PHYS_CALLING_CONVENTION,
                CallingConvention.ITERATOR,
                "PhysToIteratorRule") {
                public SaffronRel convert(SaffronRel rel)
                {
                    return new PhysToIteratorConverter(rel.getCluster(),rel);
                }
            });

        PhysLeafRel leafRel = new PhysLeafRel(newCluster(planner),"a");
        RexInputRef inputRef = new RexInputRef(
            0,
            leafRel.getRowType().getFields()[0].getType());
        ProjectRel projectRel = new ProjectRel(
            leafRel.getCluster(),
            leafRel,
            new RexNode[] {inputRef},
            new String[] {"this"},
            ProjectRel.Flags.Boxed);
        NoneSingleRel singleRel = new NoneSingleRel(
            projectRel.getCluster(),projectRel);
        SaffronRel convertedRel = planner.changeConvention(
            singleRel,CallingConvention.ITERATOR);
        planner.setRoot(convertedRel);
        SaffronRel result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysToIteratorConverter);
    }

    // NOTE:  this fails due to a problem with RemoveTrivialProjectRule
    public void testWithRemoveTrivialProject()
    {
        removeTrivialProject(true);
    }

    // NOTE:  this succeeds; it's here as constrast to
    // testWithRemoveTrivialProject()
    public void testWithoutRemoveTrivialProject()
    {
        removeTrivialProject(false);
    }

    private static class TestEnvironment extends Environment
    {
        public String toString()
        {
            return null;
        }

        public void record(String name,OJClass clazz)
        {
            throw new AssertionError();
        }

        public void bindVariable(String name,VariableInfo info)
        {
            throw new AssertionError();
        }
    }

    private static abstract class TestLeafRel extends SaffronRel
    {
        private String label;
        
        protected TestLeafRel(VolcanoCluster cluster,String label)
        {
            super(cluster);
            this.label = label;
        }

        public String getLabel()
        {
            return label;
        }
        
        // implement SaffronRel
        public Object clone()
        {
            return this;
        }
        
        // implement SaffronRel
        public PlanCost computeSelfCost(SaffronPlanner planner)
        {
            return planner.makeInfiniteCost();
        }
        
        // implement SaffronRel
        protected SaffronType deriveRowType()
        {
            return cluster.typeFactory.createProjectType(
                new SaffronType [] {
                    cluster.typeFactory.createJavaType(Void.TYPE)
                },
                new String [] {
                    "this"
                });
        }

        public void explain(PlanWriter pw)
        {
            pw.explain(
                this,
                new String [] {
                    "label" },
                new Object [] {
                    label
                });
        }
    }
    
    private static abstract class TestSingleRel extends SingleRel
    {
        protected TestSingleRel(VolcanoCluster cluster,SaffronRel child)
        {
            super(cluster,child);
        }
        
        // implement SaffronRel
        public PlanCost computeSelfCost(SaffronPlanner planner)
        {
            return planner.makeInfiniteCost();
        }
        
        // implement SaffronRel
        protected SaffronType deriveRowType()
        {
            return child.getRowType();
        }
    }

    private static class NoneSingleRel extends TestSingleRel
    {
        protected NoneSingleRel(VolcanoCluster cluster,SaffronRel child)
        {
            super(cluster,child);
        }
        
        // implement SaffronRel
        public Object clone()
        {
            return new NoneSingleRel(cluster,child);
        }
    }
    
    private static class NoneLeafRel extends TestLeafRel
    {
        protected NoneLeafRel(VolcanoCluster cluster,String label)
        {
            super(cluster,label);
        }
    }
    
    private static class PhysLeafRel extends TestLeafRel
    {
        PhysLeafRel(VolcanoCluster cluster,String label)
        {
            super(cluster,label);
        }
        
        // implement SaffronRel
        public PlanCost computeSelfCost(SaffronPlanner planner)
        {
            return planner.makeTinyCost();
        }
        
        // implement SaffronRel
        public CallingConvention getConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }
    }

    private static class PhysSingleRel extends TestSingleRel
    {
        PhysSingleRel(VolcanoCluster cluster,SaffronRel child)
        {
            super(cluster,child);
        }
        
        // implement SaffronRel
        public PlanCost computeSelfCost(SaffronPlanner planner)
        {
            return planner.makeTinyCost();
        }
        
        // implement SaffronRel
        public CallingConvention getConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }
        
        // implement SaffronRel
        public Object clone()
        {
            return new PhysSingleRel(cluster,child);
        }
    }

    class PhysToIteratorConverter extends ConverterRel
    {
        public PhysToIteratorConverter(VolcanoCluster cluster,SaffronRel child)
        {
            super(cluster,child);
        }
        
        // implement SaffronRel
        public CallingConvention getConvention()
        {
            return CallingConvention.ITERATOR;
        }

        // implement SaffronRel
        public Object clone()
        {
            return new PhysToIteratorConverter(cluster,child);
        }
    }

    private static class PhysLeafRule extends VolcanoRule
    {
        PhysLeafRule()
        {
            super(
                new RuleOperand(
                    NoneLeafRel.class,
                    null));
        }

        // implement VolcanoRule
        public CallingConvention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement VolcanoRule
        public void onMatch(VolcanoRuleCall call)
        {
            NoneLeafRel leafRel = (NoneLeafRel) call.rels[0];
            call.transformTo(
                new PhysLeafRel(leafRel.getCluster(),leafRel.getLabel()));
        }
    }
    
    private static class GoodSingleRule extends VolcanoRule
    {
        GoodSingleRule()
        {
            super(
                new RuleOperand(
                    NoneSingleRel.class,
                    new RuleOperand[] {
                        new RuleOperand(
                            SaffronRel.class,
                            null)}));
        }

        // implement VolcanoRule
        public CallingConvention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement VolcanoRule
        public void onMatch(VolcanoRuleCall call)
        {
            NoneSingleRel singleRel = (NoneSingleRel) call.rels[0];
            SaffronRel childRel = call.rels[1];
            SaffronRel physInput =
                convert(planner,childRel,PHYS_CALLING_CONVENTION);
            call.transformTo(
                new PhysSingleRel(singleRel.getCluster(),physInput));
        }
    }

    // NOTE:  BadSingleRule doesn't work because it explicitly specifies
    // PhysLeafRel rather than SaffronRel for the single input.  I'm
    // not sure if this should work (it does in other contexts)?
    private static class BadSingleRule extends VolcanoRule
    {
        BadSingleRule()
        {
            super(
                new RuleOperand(
                    NoneSingleRel.class,
                    new RuleOperand[] {
                        new RuleOperand(
                            PhysLeafRel.class,
                            null)}));
        }

        // implement VolcanoRule
        public CallingConvention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement VolcanoRule
        public void onMatch(VolcanoRuleCall call)
        {
            NoneSingleRel singleRel = (NoneSingleRel) call.rels[0];
            SaffronRel childRel = call.rels[1];
            SaffronRel physInput =
                convert(planner,childRel,PHYS_CALLING_CONVENTION);
            call.transformTo(
                new PhysSingleRel(singleRel.getCluster(),physInput));
        }
    }

    private static class PhysProjectRule extends VolcanoRule
    {
        PhysProjectRule()
        {
            super(
                new RuleOperand(
                    ProjectRel.class,
                    new RuleOperand[] {
                        new RuleOperand(
                            SaffronRel.class,
                            null)}));
        }

        // implement VolcanoRule
        public CallingConvention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement VolcanoRule
        public void onMatch(VolcanoRuleCall call)
        {
            SaffronRel childRel = call.rels[1];
            call.transformTo(
                new PhysLeafRel(childRel.getCluster(),"b"));
        }
    }
}

// End VolcanoPlannerTest.java
