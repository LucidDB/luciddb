/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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


import java.util.HashMap;

import openjava.mop.Environment;

import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;


/**
 * A <code>RelOptQuery</code> represents a set of {@link RelNode relational
 * expressions} which derive from the same <code>select</code> statement.
 */
public class RelOptQuery
{
    //~ Static fields/initializers --------------------------------------------

    /**
     * Prefix to the name of correlating variables.
     */
    public static final String correlPrefix = "$cor";

    //~ Instance fields -------------------------------------------------------

    /**
     * Maps a from-list expression to the name of the correlating variable
     * which references it. This is for forward-references, caused when from
     * items have correlating variables. We will later resolve to a {@link
     * RelNode}.
     */
    public final HashMap mapDeferredToCorrel = new HashMap();

    /**
     * Maps name of correlating variable (e.g. "$cor3") to the {@link
     * RelNode} which implements it.
     */
    final HashMap mapCorrelToRel = new HashMap();
    private final RelOptPlanner planner;
    private int nextCorrel = 0;

    //~ Constructors ----------------------------------------------------------

    public RelOptQuery(RelOptPlanner planner)
    {
        this.planner = planner;
    }

    //~ Methods ---------------------------------------------------------------

    public RelOptCluster createCluster(
        Environment env,
        RelDataTypeFactory typeFactory,
        RexBuilder rexBuilder)
    {
        return new RelOptCluster(this, env, planner, typeFactory, rexBuilder);
    }

    /**
     * Constructs a new name for a correlating variable.  It is unique within
     * the whole query.
     */
    public String createCorrel()
    {
        int n = nextCorrel++;
        return correlPrefix + n;
    }

    /**
     * Creates a name for a correlating variable for which no {@link
     * RelNode} has been created yet.
     *
     * @param deferredLookup contains the information required to resolve the
     *        variable later
     */
    public String createCorrelUnresolved(Object deferredLookup)
    {
        int n = nextCorrel++;
        String name = correlPrefix + n;
        mapDeferredToCorrel.put(deferredLookup, name);
        return name;
    }

    /**
     * Returns the relational expression which populates a correlating
     * variable.
     */
    public RelNode lookupCorrel(String name)
    {
        return (RelNode) mapCorrelToRel.get(name);
    }

    /**
     * Maps a correlating variable to a {@link RelNode}.
     */
    public void mapCorrel(
        String name,
        RelNode rel)
    {
        mapCorrelToRel.put(name, rel);
    }
}


// End RelOptQuery.java
