/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package org.eigenbase.rel.metadata;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.stat.*;


/**
 * RelMetadataQuery provides a strongly-typed facade on top of {@link
 * RelMetadataProvider} for the set of relational expression metadata queries
 * defined as standard within Eigenbase. The Javadoc on these methods serves as
 * their primary specification.
 *
 * <p>To add a new standard query <code>Xyz</code> to this interface, follow
 * these steps:
 *
 * <ol>
 * <li>Add a static method <code>getXyz</code> specification to this class.
 * <li>Add unit tests to {@link org.eigenbase.test.RelMetadataTest}.
 * <li>Write a new provider class <code>RelMdXyz</code> in this package. Follow
 * the pattern from an existing class such as {@link RelMdColumnOrigins},
 * overloading on all of the logical relational expressions to which the query
 * applies. If your new metadata query takes parameters, be sure to register
 * them in the constructor via a call to {@link
 * ReflectiveRelMetadataProvider#mapParameterTypes}.
 * <li>Register your provider class in {@link DefaultRelMetadataProvider}.
 * <li>Get unit tests working.
 * </ol>
 *
 * <p>Because relational expression metadata is extensible, extension projects
 * can define similar facades in order to specify access to custom metadata.
 * Please do not add queries here (nor on {@link RelNode}) which lack meaning
 * outside of your extension.
 *
 * <p>Besides adding new metadata queries, extension projects may need to add
 * custom providers for the standard queries in order to handle additional
 * relational expressions (either logical or physical). In either case, the
 * process is the same: write a reflective provider and chain it on to an
 * instance of {@link DefaultRelMetadataProvider}, prepending it to the default
 * providers. Then supply that instance to the planner via the appropriate
 * plugin mechanism.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class RelMetadataQuery
{

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns statistics for a relational expression. These statistics include
     * features such as row counts, or column distributions. Stats are typically
     * collected by sampling a table. They might also be inferred from a rel's
     * history. Certain rels, such as filters, might generate stats from their
     * inputs.
     *
     * @param rel the relational expression.
     *
     * @return a statistics object, if statistics are available, or null
     * otherwise
     */
    public static RelStatSource getStatistics(RelNode rel)
    {
        RelStatSource result =
            (RelStatSource) rel.getCluster().getMetadataProvider()
            .getRelMetadata(rel, "getStatistics", null);
        return result;
    }

    /**
     * Estimates the number of rows which will be returned by a relational
     * expression. The default implementation for this query asks the rel itself
     * via {@link RelNode#getRows}, but metadata providers can override this
     * with their own cost models.
     *
     * @param rel the relational expression
     *
     * @return estimated row count, or null if no reliable estimate can be
     * determined
     */
    public static Double getRowCount(RelNode rel)
    {
        Double result =
            (Double) rel.getCluster().getMetadataProvider().getRelMetadata(
                rel,
                "getRowCount",
                null);
        assert (assertNonNegative(result));
        return validateResult(result);
        
    }

    /**
     * Estimates the cost of executing a relational expression, including the
     * cost of its inputs. The default implementation for this query adds {@link
     * #getNonCumulativeCost} to the cumulative cost of each input, but metadata
     * providers can override this with their own cost models, e.g. to take into
     * account interactions between expressions.
     *
     * @param rel the relational expression
     *
     * @return estimated cost, or null if no reliable estimate can be determined
     */
    public static RelOptCost getCumulativeCost(RelNode rel)
    {
        RelOptCost result =
            (RelOptCost) rel.getCluster().getMetadataProvider().getRelMetadata(
                rel,
                "getCumulativeCost",
                null);
        return result;
    }

    /**
     * Estimates the cost of executing a relational expression, not counting the
     * cost of its inputs. (However, the non-cumulative cost is still usually
     * dependent on the row counts of the inputs.) The default implementation
     * for this query asks the rel itself via {@link RelNode#computeSelfCost},
     * but metadata providers can override this with their own cost models.
     *
     * @param rel the relational expression
     *
     * @return estimated cost, or null if no reliable estimate can be determined
     */
    public static RelOptCost getNonCumulativeCost(RelNode rel)
    {
        RelOptCost result =
            (RelOptCost) rel.getCluster().getMetadataProvider().getRelMetadata(
                rel,
                "getNonCumulativeCost",
                null);
        return result;
    }

    /**
     * Estimates the percentage of the number of rows actually produced by an
     * expression out of the number of rows it would produce if all single-table
     * filter conditions were removed.
     *
     * @param rel the relational expression
     *
     * @return estimated percentage (between 0.0 and 1.0), or null if no
     * reliable estimate can be determined
     */
    public static Double getPercentageOriginalRows(RelNode rel)
    {
        Double result =
            (Double) rel.getCluster().getMetadataProvider().getRelMetadata(
                rel,
                "getPercentageOriginalRows",
                null);
        assert (assertPercentage(result));
        return result;
    }

    /**
     * For a given output column of an expression, determines all columns of
     * underlying tables which contribute to result values. An output column may
     * have more than one origin due to expressions such as UnionRel and
     * ProjectRel. The optimizer may use this information for catalog access
     * (e.g. index availability).
     *
     * @param rel the relational expression
     * @param iOutputColumn 0-based ordinal for output column of interest
     *
     * @return set of origin columns, or null if this information cannot be
     * determined (whereas empty set indicates definitely no origin columns at
     * all)
     */
    public static Set<RelColumnOrigin> getColumnOrigins(
        RelNode rel,
        int iOutputColumn)
    {
        return
            (Set<RelColumnOrigin>) rel.getCluster().getMetadataProvider()
            .getRelMetadata(
                rel,
                "getColumnOrigins",
                new Object[] { iOutputColumn });
    }

    /**
     * Estimates the percentage of an expression's output rows which satisfy a
     * given predicate. Returns null to indicate that no reliable estimate can
     * be produced.
     *
     * @param rel the relational expression
     * @param predicate predicate whose selectivity is to be estimated against
     * rel's output
     *
     * @return estimated selectivity (between 0.0 and 1.0), or null if no
     * reliable estimate can be determined
     */
    public static Double getSelectivity(RelNode rel, RexNode predicate)
    {
        Double result =
            (Double) rel.getCluster().getMetadataProvider().getRelMetadata(
                rel,
                "getSelectivity",
                new Object[] { predicate });
        assert (assertPercentage(result));
        return result;
    }

    /**
     * Determines the set of unique minimal keys for this expression. A key is
     * represented as a BitSet, where each bit position represents a 0-based
     * output column ordinal. (Note that RelNode.isDistinct should return true
     * if and only if at least one key is known.)
     *
     * @param rel the relational expression
     *
     * @return set of keys, or null if this information cannot be determined
     * (whereas empty set indicates definitely no keys at all)
     */
    public static Set<BitSet> getUniqueKeys(RelNode rel)
    {
        return
            (Set<BitSet>) rel.getCluster().getMetadataProvider().getRelMetadata(
                rel,
                "getUniqueKeys",
                null);
    }

    /**
     * Estimates the distinct row count in the original source for the given
     * groupKey, ignoring any filtering being applied by the expression.
     * Typically, "original source" means base table, but for derived columns,
     * the estimate may come from a non-leaf rel such as a ProjectRel.
     *
     * @param rel the relational expression
     * @param groupKey column mask representing the subset of columns for which
     * the row count will be determined
     *
     * @return distinct row count for the given groupKey, or null if no reliable
     * estimate can be determined
     */
    public static Double getPopulationSize(RelNode rel, BitSet groupKey)
    {
        Double result =
            (Double) rel.getCluster().getMetadataProvider().getRelMetadata(
                rel,
                "getPopulationSize",
                new Object[] { groupKey });
        assert (assertNonNegative(result));
        return validateResult(result);
    }

    /**
     * Estimates the number of rows which would be produced by a GROUP BY on the
     * set of columns indicated by groupKey, where the input to the GROUP BY has
     * been pre-filtered by predicate. This quantity (leaving out predicate) is
     * often referred to as cardinality (as in gender being a "low-cardinality
     * column").
     *
     * @param rel the relational expression
     * @param groupKey column mask representing group by columns
     * @param predicate pre-filtered predicates
     *
     * @return distinct row count for groupKey, filtered by predicate, or null
     * if no reliable estimate can be determined
     */
    public static Double getDistinctRowCount(
        RelNode rel,
        BitSet groupKey,
        RexNode predicate)
    {
        Double result =
            (Double) rel.getCluster().getMetadataProvider().getRelMetadata(
                rel,
                "getDistinctRowCount",
                new Object[] { groupKey, predicate });
        assert (assertNonNegative(result));
        return validateResult(result);
    }

    private static boolean assertPercentage(Double result)
    {
        if (result == null) {
            return true;
        }
        double d = result.doubleValue();
        assert (d >= 0.0);
        assert (d <= 1.0);
        return true;
    }

    private static boolean assertNonNegative(Double result)
    {
        if (result == null) {
            return true;
        }
        double d = result.doubleValue();
        assert (d >= 0.0);
        return true;
    }
    
    private static Double validateResult(Double result)
    {
        if (result == null) {
            return result;
        }
        // never let the result go below 1, as it will result in incorrect
        // calculations if the rowcount is used as the denominator in a
        // division expression
        if (result < 1.0) {
            result = 1.0;
        }
        return result;
    }
}

// End RelMetadataQuery.java
