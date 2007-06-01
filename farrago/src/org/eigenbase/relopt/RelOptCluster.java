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

import openjava.mop.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * A <code>RelOptCluster</code> is a collection of {@link RelNode relational
 * expressions} which have the same environment.
 *
 * <p>See the comment against <code>net.sf.saffron.oj.xlat.QueryInfo</code> on
 * why you should put fields in that class, not this one.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since 27 September, 2001
 */
public class RelOptCluster
{
    //~ Instance fields --------------------------------------------------------

    private final Environment env;
    private final RelDataTypeFactory typeFactory;
    private final RelOptQuery query;
    private final RelOptPlanner planner;
    private RexNode originalExpression;
    private final RexBuilder rexBuilder;
    private RelMetadataProvider metadataProvider;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a cluster.
     *
     * @pre planner != null
     * @pre typeFactory != null
     */
    RelOptCluster(
        RelOptQuery query,
        Environment env,
        RelOptPlanner planner,
        RelDataTypeFactory typeFactory,
        RexBuilder rexBuilder)
    {
        assert (planner != null);
        assert (typeFactory != null);
        this.query = query;
        this.env = env;
        this.planner = planner;
        this.typeFactory = typeFactory;
        this.rexBuilder = rexBuilder;
        this.originalExpression = rexBuilder.makeLiteral("?");

        // set up a default rel metadata provider,
        // giving the planner first crack at everything
        metadataProvider = new DefaultRelMetadataProvider();
    }

    //~ Methods ----------------------------------------------------------------

    public Environment getEnv()
    {
        return env;
    }

    public RelOptQuery getQuery()
    {
        return query;
    }

    public RexNode getOriginalExpression()
    {
        return originalExpression;
    }

    public void setOriginalExpression(RexNode originalExpression)
    {
        this.originalExpression = originalExpression;
    }

    public RelOptPlanner getPlanner()
    {
        return planner;
    }

    public RelDataTypeFactory getTypeFactory()
    {
        return typeFactory;
    }

    public RexBuilder getRexBuilder()
    {
        return rexBuilder;
    }

    public RelMetadataProvider getMetadataProvider()
    {
        return metadataProvider;
    }

    /**
     * Overrides the default metadata provider for this cluster.
     *
     * @param metadataProvider custom provider
     */
    public void setMetadataProvider(RelMetadataProvider metadataProvider)
    {
        this.metadataProvider = metadataProvider;
    }
}

// End RelOptCluster.java
