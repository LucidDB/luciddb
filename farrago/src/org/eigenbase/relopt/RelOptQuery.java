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

package org.eigenbase.relopt;

import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rex.RexBuilder;
import openjava.mop.Environment;

import java.util.HashMap;

// FIXME jvs 29-Aug-2004
import com.disruptivetech.farrago.volcano.VolcanoPlannerFactory;

/**
 * A <code>RelOptQuery</code> represents a set of {@link RelNode relational
 * expressions} which derive from the same <code>select</code> statement.
 */
public class RelOptQuery
{
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
    /**
     * Prefix to the name of correlating variables.
     */
    public static final String correlPrefix = "$cor";

    //~ Constructors ----------------------------------------------------------

    public RelOptQuery()
    {
        // TODO:  get rid of threadInstances and pass planner down here
        // instead
        this(VolcanoPlannerFactory.threadInstance().newPlanner());
    }

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
        return new RelOptCluster(this,env,planner,typeFactory,rexBuilder);
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
        mapDeferredToCorrel.put(deferredLookup,name);
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
    public void mapCorrel(String name,RelNode rel)
    {
        mapCorrelToRel.put(name,rel);
    }
}


// End RelOptQuery.java
