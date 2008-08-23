/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
package net.sf.farrago.catalog;

import java.sql.*;
import java.util.*;

import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;
import org.eigenbase.stat.*;
import org.eigenbase.util14.*;


/**
 * FarragoColumnHistogram reads and interprets statistics for a column of a
 * Farrago column set. An instance of this class is returned to summarize the
 * result of applying predicate(s) to a column.
 *
 * <p>TODO: Review statistics analysis for handling of null semantics. Null
 * values are less than all other values (e.g. bars might contain the starting
 * values null,0,1,...). Because stats analysis is based on ranges, only ranges
 * which include consecutive bars are supported. Examples:
 *
 * <ul>
 * <li>if NULL_MATCHES_ANYTHING, and querying for "col = 5", then match either
 * null or 5. Two intervals are required [null,null] and [5,5]
 * <li>if NULL_MATCHES_NULL, and querying "col = null" then null may be matched
 * with the point interval [null,null]
 * <li>otherwise, null cannot be matched, so a query like "col &lt; 1" becomes
 * (null,1)
 * </ul>
 *
 * @author John Pham
 * @version $Id$
 */
public class FarragoColumnHistogram
    implements RelStatColumnStatistics
{
    //~ Instance fields --------------------------------------------------------

    private FemAbstractColumn column;
    private SargIntervalSequence sequence;
    private Timestamp labelTimestamp;
    private FemColumnHistogram histogram;
    private int barCount;
    private List<FemColumnHistogramBar> bars;

    Double selectivity;
    Double cardinality;

    //~ Constructors -----------------------------------------------------------

    /**
     * Initializes a column statistics reader. The statistics are not actually
     * analyzed until the user calls {@link #evaluate()}.
     *
     * @deprecated
     * 
     * @param column column to analyze
     * @param sequence optional predicate on the column
     */
    protected FarragoColumnHistogram(
        FemAbstractColumn column,
        SargIntervalSequence sequence)
    {
        this(column, sequence, null);
    }
    
    /**
     * Initializes a column statistics reader. The statistics are not actually
     * analyzed until the user calls {@link #evaluate()}.
     *
     * @param column column to analyze
     * @param sequence optional predicate on the column
     * @param labelTimestamp the creation timestamp of the label setting that
     * determines which set of stats should be used; null if there is no label
     * setting
     */
    protected FarragoColumnHistogram(
        FemAbstractColumn column,
        SargIntervalSequence sequence,
        Timestamp labelTimestamp)
    {
        this.column = column;
        this.sequence = sequence;
        this.labelTimestamp = labelTimestamp;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelStatColumnStatistics
    public Double getSelectivity()
    {
        return selectivity;
    }

    // implement RelStatColumnStatistics
    public Double getCardinality()
    {
        return cardinality;
    }

    /**
     * Analyzes column histogram to determine the selectivity and cardinality of
     * the specified search condition.
     */
    protected void evaluate()
    {
        histogram = FarragoCatalogUtil.getHistogram(column, labelTimestamp);
        if (histogram == null) {
            return;
        }

        Long valueCount = histogram.getDistinctValueCount();
        if (valueCount == null) {
            selectivity = (sequence == null) ? 1.0 : null;
            cardinality = null;
            return;
        }
        
        if (sequence == null) {
            selectivity = 1.0;
            cardinality = Double.valueOf(valueCount);
            return;
        }

        barCount = histogram.getBarCount();
        bars = histogram.getBar();
        assert (bars.size() == barCount) : "invalid histogram bar count";

        List<HistogramBarCoverage> coverages = getCoverage(sequence);
        
        if (coverages == null) {
            selectivity = null;
            cardinality = null;
            return;
        }
        
        readCoverages(coverages);
    }

    /**
     * Computes the histogram bar coverage of an ordered sequence of intervals.
     * Coverage can only be computed if the end points of each interval in
     * the sequence are either literal or infinite.
     * 
     * @param sequence sequence to lookup
     * @return List of HistogramBarCoverage instance of null if coverage
     *         cannot be computed.
     */
    private List<HistogramBarCoverage> getCoverage(
        SargIntervalSequence sequence)
    {
        List<HistogramBarCoverage> coverages =
            new ArrayList<HistogramBarCoverage>(barCount);
        for (int i = 0; i < barCount; i++) {
            coverages.add(new HistogramBarCoverage());
        }

        int minBar = 0;
        for (SargInterval interval : sequence.getList()) {
            if (!checkEndpoint(interval.getLowerBound()) ||
                !checkEndpoint(interval.getUpperBound()))
            {
                // Can't handle non-literal endpoints, signal the caller.
                return null;
            }
            
            HistogramRange range = new HistogramRange(bars, interval, minBar);
            range.evaluate();
            if (range.isEmpty()) {
                continue;
            }

            String minVal = bars.get(range.firstBar).getStartingValue();
            HistogramBarCoverage.addRange(coverages, range, minVal);
            minBar = range.getLastBar();
        }
        return coverages;
    }

    /**
     * Check if the given SargEndpoint is infinite or is bounded by a literal
     * expression.
     * 
     * @param endpoint the endpoint to evaluate
     * @return true if the endpoint is infinite or bounded by a literal; false
     *         otherwise
     */
    private boolean checkEndpoint(SargEndpoint endpoint)
    {
        return 
            !endpoint.isFinite() || 
            endpoint.getCoordinate() instanceof RexLiteral;
    }
    
    /**
     * Reads collective coverages finally make estimates on requested
     * attributes. This implementation looks at each bar separately, estimating
     * how much of each bar is covered. It then accounts for that bar's
     * contribution to the entire results.
     *
     * @param coverages list of coverage for each bar
     */
    private void readCoverages(List<HistogramBarCoverage> coverages)
    {
        // determine a correction factor:
        //     actual values = sampled values * correction
        // For computed statistics, the correction will be 1.0.  For estimated
        // statistics it will be >= 1.0.
        Long histValues = histogram.getDistinctValueCount();
        Long sampleValues = 0L;
        for (FemColumnHistogramBar bar : bars) {
            sampleValues += bar.getValueCount();
        }
        assert (histValues >= sampleValues);
        Double correction =
            NumberUtil.divide(
                Double.valueOf(histValues),
                Double.valueOf(sampleValues));

        Double totalFraction = 0.0;
        Double totalValues = 0.0;
        for (int i = 0; i < coverages.size(); i++) {
            // estimate the actual cardinality of the bar
            Double barValues =
                NumberUtil.multiply(
                    Double.valueOf(bars.get(i).getValueCount()),
                    correction);

            HistogramBarCoverage coverage = coverages.get(i);
            Double fraction = coverage.estimateFraction(barValues);

            // the last bar may contain a few less rows than the others
            if (i == (coverages.size() - 1)) {
                fraction =
                    NumberUtil.multiply(
                        fraction,
                        ((double) histogram.getRowsLastBar())
                        / histogram.getRowsPerBar());
            }
            Double values = coverage.estimateCardinality(barValues);
            totalFraction = NumberUtil.add(totalFraction, fraction);
            totalValues = NumberUtil.add(totalValues, values);
        }
        totalFraction = NumberUtil.divide(totalFraction, (double) bars.size());
        selectivity = totalFraction;
        cardinality = totalValues;
    }

    /**
     * Compares a value in a histogram with a Sarg coordinate
     *
     * @param histValue a histogram value
     * @param coordinate a sarg coordinate, or a null pointer to represent the
     * null value. An infinite coordinate is not recognized by this function.
     *
     * @return -1 if value is less than point, 0 if value equals point, or 1 if
     * value is greater than point
     */
    protected static int compare(String histValue, RexLiteral coordinate)
    {
        // treat null values as the least value
        Comparable sargValue =
            (coordinate == null) ? null : coordinate.getValue();
        if ((histValue == null) && (sargValue == null)) {
            return 0;
        } else if (histValue == null) {
            return -1;
        } else if (sargValue == null) {
            return 1;
        }

        RexLiteral rexValue =
            RexLiteral.fromJdbcString(
                coordinate.getType(),
                coordinate.getTypeName(),
                histValue);

        int comparison = rexValue.getValue().compareTo(sargValue);
        return comparison;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Histogram range represents a set of bars in a histogram
     */
    private class HistogramRange
    {
        private List<FemColumnHistogramBar> bars;
        private SargInterval interval;
        private int minBar;

        private boolean empty;
        private int firstBar;
        private int lastBar;

        /**
         * Initializes a range of histogram bars spanning a search interval
         *
         * @param bars histogram bars to search
         * @param interval the search interval
         * @param minBar the first bar to search
         */
        protected HistogramRange(
            List<FemColumnHistogramBar> bars,
            SargInterval interval,
            int minBar)
        {
            this.bars = bars;
            this.interval = interval;
            this.minBar = minBar;
        }

        /**
         * Searches through the histogram to find the first and last bars the
         * interval may cover in the histogram. This range may be empty.
         */
        protected void evaluate()
        {
            int start =
                findStartBar(
                    minBar,
                    interval.getLowerBound());
            int end =
                findEndBar(
                    start,
                    interval.getUpperBound());

            if (start == end) {
                empty = true;
                firstBar = lastBar = -1;
            } else {
                empty = false;
                firstBar = start;
                lastBar = end - 1;
            }
        }

        /**
         * Returns whether the range is empty
         */
        protected boolean isEmpty()
        {
            return empty;
        }

        /**
         * Returns the zero based index of the first bar in the range, or -1 if
         * the range is empty.
         */
        protected int getFirstBar()
        {
            return firstBar;
        }

        /**
         * Returns the zero based index of the last bar in the range, or -1 if
         * the range is empty.
         */
        protected int getLastBar()
        {
            return lastBar;
        }

        /**
         * Finds the first histogram bar which may contain the specified
         * starting search point, or points greater than the search point. We
         * can rule out histogram bars which end before the search point.
         * However, we can never rule out the last histogram bar, because it has
         * no definite end.
         *
         * @param min index of first histogram bar to search
         * @param point start point to search for
         *
         * @return index of first bar not containing point
         */
        private int findStartBar(int min, SargEndpoint point)
        {
            // if the start is "-infinity" then the range starts at the
            // first bar
            if (!point.isFinite()) {
                return 0;
            }
            RexLiteral coordinate =
                point.isNull() ? null : (RexLiteral) point.getCoordinate();
            boolean open = (point.getStrictness() == SargStrictness.OPEN);

            int start;
            for (start = min; (start + 1) < bars.size(); start++) {
                // The end of the current bar can be anything up to the start
                // of the next bar, inclusive. Here we skip past bars that are
                // less than the search value.
                FemColumnHistogramBar nextBar = bars.get(start + 1);
                int comparison =
                    compare(
                        nextBar.getStartingValue(),
                        coordinate);
                if ((comparison < 0) || (open && (comparison == 0))) {
                    continue;
                }
                break;
            }
            return start;
        }

        /**
         * Finds the first histogram bar which does not contain the specified
         * point. A bar which does not contain the end point will have a
         * starting value greater than the point.
         *
         * <p>If all bars may contain the search point, returns the index of the
         * last bar + 1.
         *
         * @param min index of first histogram bar to search
         * @param point point to search for
         *
         * @return index of first bar not containing point
         */
        private int findEndBar(int min, SargEndpoint point)
        {
            // return end for "+infinity"
            if (!point.isFinite()) {
                return bars.size();
            }
            RexLiteral coordinate =
                point.isNull() ? null : (RexLiteral) point.getCoordinate();
            boolean open = (point.getStrictness() == SargStrictness.OPEN);

            int end;
            for (end = min; end < bars.size(); end++) {
                // if the histogram bar starts starts after the point
                // we know the bar does not contain the point
                FemColumnHistogramBar bar = bars.get(end);
                int comparison =
                    compare(
                        bar.getStartingValue(),
                        coordinate);
                if ((comparison > 0) || (open && (comparison == 0))) {
                    break;
                }
            }
            return end;
        }
    }

    /**
     * Describes which points and ranges lie on a histogram bar
     */
    private static class HistogramBarCoverage
    {
        private boolean entireBar;

        private int cardinalityPoint;
        private int cardinalityRanges;

        private int selectivityPoints;
        private int selectivityRanges;

        /**
         * Determines how a histogram range, possibly spanning several bars,
         * covers each histogram bar, and accumulates a running total of the
         * coverage.
         *
         * @param coverages set of coverages for each bar
         * @param range histogram bar range to be tabulated
         */
        protected static void addRange(
            List<HistogramBarCoverage> coverages,
            HistogramRange range,
            String minVal)
        {
            // continue if range does not overlap any bars
            if (range.isEmpty()) {
                return;
            }

            // cardinality fields
            if (range.interval.isPoint()) {
                coverages.get(range.firstBar).cardinalityPoint++;
            } else {
                for (int i = range.firstBar; i <= range.lastBar; i++) {
                    coverages.get(i).cardinalityRanges++;
                }
            }

            // selectivity fields (handled separately since we are
            // in the case where points span multiple bars)
            if (range.firstBar == range.lastBar) {
                if (range.interval.isPoint()) {
                    coverages.get(range.firstBar).selectivityPoints++;
                } else {
                    coverages.get(range.firstBar).selectivityRanges++;
                }

                // return since single bar ranges rarely cover an entire bar
                return;
            }
            for (int i = range.firstBar; i <= range.lastBar; i++) {
                coverages.get(i).selectivityRanges++;
            }

            // look for entire bars. the first bar of the range is fully
            // covered if the lower bound is infinite or less than the
            // first bar of the histogram range (note that the range
            // has at least two bars)
            assert (range.firstBar != range.lastBar);
            SargEndpoint lower = range.interval.getLowerBound();
            if (!lower.isFinite()) {
                assert (range.firstBar == 0) : "-infinity does not span first histogram bar";
                coverages.get(0).entireBar = true;
            } else {
                RexLiteral coordinate = (RexLiteral) lower.getCoordinate();
                int comparison = compare(minVal, coordinate);
                if ((comparison == 1)
                    || ((comparison == 0) && lower.isClosed()))
                {
                    coverages.get(0).entireBar = true;
                }
            }

            // all intermediate bars are fully covered
            for (int i = range.firstBar + 1; i < range.lastBar; i++) {
                coverages.get(i).entireBar = true;
            }

            // the last bar may be covered if there is no upper bound
            if (!range.interval.getUpperBound().isFinite()) {
                assert (range.lastBar == (coverages.size() - 1)) : "+infinity does not span last histogram bar";
                coverages.get(range.lastBar).entireBar = true;
            }
        }

        /**
         * Estimates what percentage of a bar is covered, based on the bar's
         * total cardinality.
         *
         * @param cardinality the expected cardinality of the bar
         *
         * @return an estimate from 0.0 to 1.0 representing a fraction of a bar,
         * or null if no reliable estimate can be made
         */
        protected Double estimateFraction(
            Double cardinality)
        {
            if (entireBar) {
                return Double.valueOf(1);
            }

            // if only points are provided, cardinality is required to
            // make an educated guess with them
            if ((cardinality == null) && (selectivityRanges == 0)) {
                return null;
            }

            // start with the contribution by points contained by bar
            double fraction = 0;
            if ((cardinality != null) && (selectivityPoints > 0)) {
                // assume cardinality is at least 1 for the division
                cardinality = Math.max(cardinality, 1.0);
                fraction = Math.min(selectivityPoints / cardinality, 1.0);
            }

            // add in a half for any remaining points
            double remaining = 1 - fraction;
            if (selectivityRanges > 0) {
                fraction += remaining / 2;
            }
            return fraction;
        }

        /**
         * Estimates the cardinality of the column after applying a predicate.
         *
         * @param cardinality the expected cardinality of the bar
         *
         * @return an estimate of the cardinality of the column after applying
         * the predicate, or null if no reliable estimate can be made
         */
        protected Double estimateCardinality(Double cardinality)
        {
            double estimatedBarCardinality =
                (cardinality != null) ? cardinality : 0;
            assert (estimatedBarCardinality >= 0) : "histogram bar contained negative number of distinct values";

            // Start with the points as a basis for cardinality.
            // Return this if it represents the entire bar, or if
            // there are no ranges.
            double points = cardinalityPoint;
            if (((points > 0) && (points >= estimatedBarCardinality))
                || (cardinalityRanges == 0))
            {
                return points;
            }

            // Otherwise, there is some kind of range and we need the bar's
            // estimated cardinality to resolve our filtered cardinality
            if (cardinality == null) {
                return null;
            }

            // A range spanning the entire bar gets half the cardinality,
            // while other ranges get half of the remaining points
            if (entireBar) {
                points = Math.max(estimatedBarCardinality, points);
            }

            // in case we have an empty bar
            double remainingPoints =
                Math.max(estimatedBarCardinality - points, 0.0);
            return Double.valueOf(points + (remainingPoints / 2));
        }
    }
}

// End FarragoColumnHistogram.java
