/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

import openjava.mop.Environment;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexToSqlTranslator;


/**
 * A <code>RelOptCluster</code> is a collection of {@link RelNode relational
 * expressions } which have the same environment.
 *
 * <p>
 * See the comment against <code>net.sf.saffron.oj.xlat.QueryInfo</code> on
 * why you should put fields in that class, not this one.
 * </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 27 September, 2001
 */
public class RelOptCluster
{
    //~ Instance fields -------------------------------------------------------

    public final Environment env;
    public final RelDataTypeFactory typeFactory;
    public final RelOptQuery query;
    public RelOptPlanner planner;
    public RexNode originalExpression;
    public final RexBuilder rexBuilder;
    public RexToSqlTranslator rexToSqlTranslator;

    //~ Constructors ----------------------------------------------------------

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
    }

    //~ Methods ---------------------------------------------------------------

    public RexNode getOriginalExpression()
    {
        return originalExpression;
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
}


// End RelOptCluster.java
