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

import net.sf.saffron.core.PlanWriter;
import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rel.convert.ConverterRel;


/**
 * Converts a relational expression to any given output convention.
 *
 * <p>
 * Unlike most {@link ConverterRel}s, and abstract converter is always
 * abstract. You would typically create an <code>AbstractConverter</code>
 * when it is necessary to transform a relational expression immediately;
 * later, rules will transform it into relational expressions which can be
 * implemented.
 * </p>
 *
 * <p>
 * If an abstract converter cannot be satisfied immediately (because the
 * source subset is abstract), the set is flagged, so this converter will be
 * expanded as soon as a non-abstract relexp is added to the set.
 * </p>
 */
public class AbstractConverter extends ConverterRel
{
    //~ Instance fields -------------------------------------------------------

    final CallingConvention outConvention;

    //~ Constructors ----------------------------------------------------------

    public AbstractConverter(
        VolcanoCluster cluster,
        SaffronRel rel,
        CallingConvention outConvention)
    {
        super(cluster,rel);
        this.outConvention = outConvention;
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return outConvention;
    }

    public Object clone()
    {
        return new AbstractConverter(cluster,child,outConvention);
    }

    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        return planner.makeInfiniteCost();
    }

    public void explain(PlanWriter pw)
    {
        pw.explain(
            this,
            new String [] { "child","convention" },
            new Object [] { outConvention });
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Rule which converts an {@link AbstractConverter} into a chain of
     * converters from the source relation to the target calling convention.
     *
     * <p>
     * The chain produced is mimimal: we have previously built the transitive
     * closure of the graph of conversions, so we choose the shortest chain.
     * </p>
     *
     * <p>
     * Unlike the {@link AbstractConverter} they are replacing, these
     * converters are guaranteed to be able to convert any relation of their
     * calling convention. Furthermore, because they introduce subsets of
     * other calling conventions along the way, these subsets may spawn more
     * efficient conversions which are not generally applicable.
     * </p>
     *
     * <p>
     * AbstractConverters can be messy, so they restrain themselves: they
     * don't fire if the target subset already has an implementation (with
     * less than infinite cost).
     * </p>
     */
    public static class ExpandConversionRule extends VolcanoRule
    {
        public ExpandConversionRule()
        {
            super(
                new RuleOperand(
                    AbstractConverter.class,
                    new RuleOperand [] { new RuleOperand(
                            SaffronRel.class,
                            null) }));
        }

        public void onMatch(VolcanoRuleCall call)
        {
            AbstractConverter converter = (AbstractConverter) call.rels[0];
            final RelSubset converterSubset = planner.getSubset(converter);
            if (!planner.getCost(converterSubset).isInfinite()) {
                return;
            }
            final SaffronRel child = converter.child;
            SaffronRel converted =
                planner.changeConventionUsingConverters(
                    child,
                    converter.outConvention);
            if (converted != null) {
                call.transformTo(converted);
                return;
            }
            if (true) {
                return;
            }

            // Since we couldn't convert directly, create abstract converters to
            // all sibling subsets. This will cause them to be important, and
            // hence rules will fire which may generate the conversion we need.
            final RelSet set = getPlanner().getSet(child);
            for (int i = 0; i < set.subsets.size(); i++) {
                RelSubset subset = (RelSubset) set.subsets.get(i);
                if (
                    (subset.getConvention() == child.getConvention())
                        || (subset.getConvention() == converter.outConvention)) {
                    continue;
                }
                final AbstractConverter newConverter =
                    new AbstractConverter(
                        child.getCluster(),
                        subset,
                        converter.outConvention);
                call.transformTo(newConverter);
            }
        }
    }
}


// End AbstractConverter.java
