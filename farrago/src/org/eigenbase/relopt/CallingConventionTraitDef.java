/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
import org.eigenbase.rel.convert.*;
import org.eigenbase.util.*;


/**
 * CallingConventionTraitDef is a {@link RelTraitDef} that defines the
 * calling-convention trait. A new set of conversion information is created for
 * each planner that registers at least one {@link ConverterRule} instance.
 *
 * <p>Conversion data is held in a {@link WeakHashMap} so that the JVM's garbage
 * collector may reclaim the conversion data after the planner itself has been
 * garbage collected. The conversion information consists of a graph of
 * conversions (from one calling convention to another) and a map of graph arcs
 * to {@link ConverterRule}s.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class CallingConventionTraitDef
    extends RelTraitDef
{
    //~ Static fields/initializers ---------------------------------------------

    public static final CallingConventionTraitDef instance =
        new CallingConventionTraitDef();

    //~ Instance fields --------------------------------------------------------

    /**
     * Weak-key map of RelOptPlanner to ConversionData. The idea is that when
     * the planner goes away, so does the map entry.
     */
    private final WeakHashMap<RelOptPlanner, ConversionData>
        plannerConversionMap = new WeakHashMap<RelOptPlanner, ConversionData>();

    //~ Constructors -----------------------------------------------------------

    private CallingConventionTraitDef()
    {
        super();
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelTraitDef
    public Class getTraitClass()
    {
        return CallingConvention.class;
    }

    // implement RelTraitDef
    public String getSimpleName()
    {
        return "convention";
    }

    // override RelTraitDef
    public void registerConverterRule(
        RelOptPlanner planner,
        ConverterRule converterRule)
    {
        if (converterRule.isGuaranteed()) {
            ConversionData conversionData = getConversionData(planner);

            final Graph<CallingConvention> conversionGraph =
                conversionData.conversionGraph;
            final MultiMap<Graph.Arc, ConverterRule> mapArcToConverterRule =
                conversionData.mapArcToConverterRule;

            final Graph.Arc arc =
                conversionGraph.createArc(
                    (CallingConvention) converterRule.getInTrait(),
                    (CallingConvention) converterRule.getOutTrait());

            mapArcToConverterRule.putMulti(arc, converterRule);
        }
    }

    public void deregisterConverterRule(
        RelOptPlanner planner,
        ConverterRule converterRule)
    {
        if (converterRule.isGuaranteed()) {
            ConversionData conversionData = getConversionData(planner);

            final Graph<CallingConvention> conversionGraph =
                conversionData.conversionGraph;
            final MultiMap<Graph.Arc, ConverterRule> mapArcToConverterRule =
                conversionData.mapArcToConverterRule;

            final Graph.Arc arc =
                conversionGraph.deleteArc(
                    (CallingConvention) converterRule.getInTrait(),
                    (CallingConvention) converterRule.getOutTrait());
            assert arc != null;

            mapArcToConverterRule.removeMulti(arc, converterRule);
        }
    }

    // implement RelTraitDef
    public RelNode convert(
        RelOptPlanner planner,
        RelNode rel,
        RelTrait toTrait,
        boolean allowInfiniteCostConverters)
    {
        final ConversionData conversionData = getConversionData(planner);
        final Graph<CallingConvention> conversionGraph =
            conversionData.conversionGraph;
        final MultiMap<Graph.Arc, ConverterRule> mapArcToConverterRule =
            conversionData.mapArcToConverterRule;

        final CallingConvention fromConvention = rel.getConvention();
        final CallingConvention toConvention = (CallingConvention) toTrait;

        Iterator<Graph.Arc<CallingConvention>[]> conversionPaths =
            conversionGraph.getPaths(fromConvention, toConvention);

loop:
        while (conversionPaths.hasNext()) {
            Graph.Arc [] arcs = conversionPaths.next();
            assert (arcs[0].from == fromConvention);
            assert (arcs[arcs.length - 1].to == toConvention);
            RelNode converted = rel;
            for (int i = 0; i < arcs.length; i++) {
                if (planner.getCost(converted).isInfinite()
                    && !allowInfiniteCostConverters)
                {
                    continue loop;
                }
                converted =
                    changeConvention(
                        converted,
                        arcs[i],
                        mapArcToConverterRule);
                if (converted == null) {
                    throw Util.newInternal(
                        "Converter from " + arcs[i].from
                        + " to " + arcs[i].to
                        + " guaranteed that it could convert any relexp");
                }
            }
            return converted;
        }

        return null;
    }

    /**
     * Tries to convert a relational expression to the target convention of an
     * arc.
     */
    private RelNode changeConvention(
        RelNode rel,
        Graph.Arc arc,
        final MultiMap<Graph.Arc, ConverterRule> mapArcToConverterRule)
    {
        assert (arc.from == rel.getConvention());

        // Try to apply each converter rule for this arc's source/target calling
        // conventions.
        for (
            Iterator<ConverterRule> converterRuleIter =
                mapArcToConverterRule.getMulti(arc).iterator();
            converterRuleIter.hasNext();)
        {
            ConverterRule converterRule = converterRuleIter.next();
            assert (converterRule.getInTrait() == arc.from);
            assert (converterRule.getOutTrait() == arc.to);
            RelNode converted = converterRule.convert(rel);
            if (converted != null) {
                return converted;
            }
        }
        return null;
    }

    // implement RelTraitDef
    public boolean canConvert(
        RelOptPlanner planner,
        RelTrait fromTrait,
        RelTrait toTrait)
    {
        ConversionData conversionData = getConversionData(planner);

        CallingConvention fromConvention = (CallingConvention) fromTrait;
        CallingConvention toConvention = (CallingConvention) toTrait;

        return conversionData.conversionGraph.getShortestPath(
            fromConvention,
            toConvention) != null;
    }

    private ConversionData getConversionData(RelOptPlanner planner)
    {
        if (plannerConversionMap.containsKey(planner)) {
            return plannerConversionMap.get(planner);
        }

        // Create new, empty ConversionData
        ConversionData conversionData = new ConversionData();
        plannerConversionMap.put(planner, conversionData);
        return conversionData;
    }

    //~ Inner Classes ----------------------------------------------------------

    private static final class ConversionData
    {
        final Graph<CallingConvention> conversionGraph =
            new Graph<CallingConvention>();

        /**
         * For a given source/target convention, there may be several possible
         * conversion rules. Maps {@link org.eigenbase.util.Graph.Arc} to a
         * collection of {@link ConverterRule} objects.
         */
        final MultiMap<Graph.Arc, ConverterRule> mapArcToConverterRule =
            new MultiMap<Graph.Arc, ConverterRule>();
    }
}

// End CallingConventionTraitDef.java
