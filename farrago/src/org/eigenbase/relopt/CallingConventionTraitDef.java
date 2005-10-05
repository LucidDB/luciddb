/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.util.Graph;
import org.eigenbase.util.MultiMap;
import org.eigenbase.util.Util;

import java.util.Iterator;
import java.util.WeakHashMap;

/**
 * CallingConventionTraitDef is a {@link RelTraitDef} that defines the
 * calling-convention trait.  A new set of conversion information is created
 * for each planner that registers at least one {@link ConverterRule}
 * instance.
 *
 * <p>Conversion data is held in a {@link WeakHashMap} so that the JVM's
 * garbage collector may reclaim the conversion data after the planner itself
 * has been garbage collected.  The conversion information consists of a
 * graph of conversions (from one calling convention to another) and a map
 * of graph arcs to {@link ConverterRule}s.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class CallingConventionTraitDef extends RelTraitDef
{
    public static final CallingConventionTraitDef instance =
        new CallingConventionTraitDef();


    /**
     * Weak-key map of RelOptPlanner to ConversionData.  The idea is that
     * when the planner goes away, so does the map entry.
     */
    private final WeakHashMap plannerConversionMap = new WeakHashMap();


    private CallingConventionTraitDef()
    {
        super();
    }

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
        RelOptPlanner planner, ConverterRule converterRule)
    {
        if (converterRule.isGuaranteed()) {
            ConversionData conversionData = getConversionData(planner);

            final Graph conversionGraph = conversionData.conversionGraph;
            final MultiMap mapArcToConverterRule =
                conversionData.mapArcToConverterRule;

            final Graph.Arc arc =
                conversionGraph.createArc(
                    (CallingConvention)converterRule.getInTraits().getTrait(this),
                    (CallingConvention)converterRule.getOutTraits().getTrait(this));

            mapArcToConverterRule.putMulti(arc, converterRule);
        }
    }

    public void deregisterConverterRule(
        RelOptPlanner planner, ConverterRule converterRule)
    {
        if (converterRule.isGuaranteed()) {
            ConversionData conversionData = getConversionData(planner);

            final Graph conversionGraph = conversionData.conversionGraph;
            final MultiMap mapArcToConverterRule =
                conversionData.mapArcToConverterRule;

            final Graph.Arc arc = conversionGraph.deleteArc(
                (CallingConvention)converterRule.getInTraits().getTrait(this),
                (CallingConvention)converterRule.getOutTraits().getTrait(this));
            assert arc != null;

            mapArcToConverterRule.removeMulti(arc, converterRule);
        }
    }

    // implement RelTraitDef
    public RelNode convert(
        RelOptPlanner planner, RelNode rel, RelTrait toTrait,
        boolean allowInfiniteCostConverters)
    {
        final ConversionData conversionData = getConversionData(planner);
        final Graph conversionGraph = conversionData.conversionGraph;
        final MultiMap mapArcToConverterRule =
            conversionData.mapArcToConverterRule;

        final CallingConvention fromConvention = rel.getConvention();
        final CallingConvention toConvention = (CallingConvention)toTrait;

        Iterator conversionPaths =
            conversionGraph.getPaths(fromConvention, toConvention);

loop:
        while (conversionPaths.hasNext()) {
            Graph.Arc [] arcs = (Graph.Arc []) conversionPaths.next();
            assert (arcs[0].from == fromConvention);
            assert (arcs[arcs.length - 1].to == toConvention);
            RelNode converted = rel;
            for (int i = 0; i < arcs.length; i++) {
                if (planner.getCost(converted).isInfinite()
                        && !allowInfiniteCostConverters) {
                    continue loop;
                }
                converted = changeConvention(
                    converted, arcs[i], mapArcToConverterRule);
                if (converted == null) {
                    throw Util.newInternal("Converter from " + arcs[i].from
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
        final MultiMap mapArcToConverterRule)
    {
        assert (arc.from == rel.getConvention());

        // Try to apply each converter rule for this arc's source/target calling
        // conventions.
        for (Iterator converterRuleIter =
                mapArcToConverterRule.getMulti(arc).iterator();
                converterRuleIter.hasNext();) {
            ConverterRule converterRule =
                (ConverterRule) converterRuleIter.next();
            assert (converterRule.getInTraits().getTrait(this) == arc.from);
            assert (converterRule.getOutTraits().getTrait(this) == arc.to);
            RelNode converted = converterRule.convert(rel);
            if (converted != null) {
                return converted;
            }
        }
        return null;
    }


    // implement RelTraitDef
    public boolean canConvert(
        RelOptPlanner planner, RelTrait fromTrait, RelTrait toTrait)
    {
        ConversionData conversionData = getConversionData(planner);

        CallingConvention fromConvention = (CallingConvention)fromTrait;
        CallingConvention toConvention = (CallingConvention)toTrait;

        return
            conversionData.conversionGraph.getShortestPath(
                fromConvention, toConvention) != null;
    }


    private ConversionData getConversionData(RelOptPlanner planner)
    {
        if (plannerConversionMap.containsKey(planner)) {
            return (ConversionData)plannerConversionMap.get(planner);
        }

        // Create new, empty ConversionData
        ConversionData conversionData = new ConversionData();
        plannerConversionMap.put(planner, conversionData);
        return conversionData;
    }


    private static final class ConversionData
    {
        final Graph conversionGraph = new Graph();

        /**
         * For a given source/target convention, there may be several possible
         * conversion rules. Maps {@link Graph.Arc} to a collection of
         * {@link ConverterRule} objects.
         */
        final MultiMap mapArcToConverterRule = new MultiMap();
    }
}

// End CallingConventionTraitDef.java
