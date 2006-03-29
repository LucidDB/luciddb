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

import org.eigenbase.rel.*;
import org.eigenbase.rex.*;

import java.util.*;

/**
 * RelMetadataQuery provides a strongly-typed facade on top of {@link
 * RelMetadataProvider} for the set of relational expression metadata
 * queries defined as standard within Eigenbase.  The Javadoc on these methods
 * serves as their primary specification.
 *
 *<p>
 *
 * To add a new standard query <code>Xyz</code> to this interface, follow these
 * steps:
 *
 *<ol>
 *
 * <li>Add a static method <code>getXyz</code> specification to this class.
 *
 * <li>Add unit tests to {@link org.eigenbase.test.RelMetadataTest}.
 *
 * <li>Write a new provider class <code>RelMdXyz</code> in this package.
 * Follow the pattern from an existing class such as {@link
 * RelMdColumnOrigins}, *overloading on all of the logical relational
 * expressions to which the query *applies.  If your new metadata query takes
 * parameters, be sure to register *them in the constructor via a call to
 * {@link *ReflectiveRelMetadataProvider#mapParameterTypes}.
 *
 * <li>Register your provider class in {@link DefaultRelMetadataProvider}.
 *
 * <li>Get unit tests working.
 *
 *</ol>
 *
 *<p>
 *
 * Because relational expression metadata is extensible, extension projects
 * can define similar facades in order to specify access to custom metadata.
 * Please do not add queries here (nor on {@link RelNode}) which lack meaning
 * outside of your extension.
 *
 *<p>
 *
 * Besides adding new metadata queries, extension projects may need to add
 * custom providers for the standard queries in order to handle additional
 * relational expressions (either logical or physical).  In either case,
 * the process is the same:  write a reflective provider and chain
 * it on to an instance of {@link DefaultRelMetadataProvider}, prepending
 * it to the default providers.  Then supply that instance to the
 * planner via the appropriate plugin mechanism.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class RelMetadataQuery
{
    /**
     * Estimates the percentage of the number of rows actually produced by an
     * expression out of the number of rows it would produce if all
     * single-table filter conditions were removed.
     *
     * @param rel the relational expression
     *
     * @return estimated percentage (between 0.0 and 1.0), or
     * null if no reliable estimate can be determined
     */
    public static Double getPercentageOriginalRows(RelNode rel)
    {
        Double result = 
            (Double) rel.getCluster().getMetadataProvider().getRelMetadata(
                rel, "getPercentageOriginalRows", null);
        assert(assertPercentage(result));
        return result;
    }

    /**
     * For a given output column of an expression, determines all
     * columns of underlying tables which contribute to result values. An
     * ouptut column may have more than one origin due to expressions such as
     * UnionRel and ProjectRel. The optimizer may use this information for
     * catalog access (e.g. index availability).
     *
     * @param rel the relational expression
     *
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
        return (Set<RelColumnOrigin>)
            rel.getCluster().getMetadataProvider().getRelMetadata(
                rel, "getColumnOrigins", new Object [] { iOutputColumn });
    }

    // TODO jvs 29-Mar-2006:  I haven't yet added the providers
    // for getSelectivity and some other specified in wiki.
    
    /**
     * Estimates the percentage of an expression's output rows which satisfy
     * a given predicate. Returns null to indicate that no reliable estimate
     * can be produced.
     *
     * @param rel the relational expression
     *
     * @param predicate predicate whose selectivity is to be estimated against
     * rel's output
     *
     * @return estimated selectivity (between 0.0 and 1.0), or
     * null if no reliable estimate can be determined
     */
    public static Double getSelectivity(RelNode rel, RexNode predicate)
    {
        Double result = 
            (Double) rel.getCluster().getMetadataProvider().getRelMetadata(
                rel, "getSelectivity", new Object [] { predicate });
        assert(assertPercentage(result));
        return result;
    }

    private static boolean assertPercentage(Double result)
    {
        if (result == null) {
            return true;
        }
        double d = result.doubleValue();
        assert(d >= 0.0);
        assert(d <= 1.0);
        return true;
    }
}

// End RelMetadataQuery.java
