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

import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rex.RexBuilder;
import net.sf.saffron.rex.RexToSqlTranslator;
import net.sf.saffron.rex.RexNode;
import openjava.mop.Environment;

/**
 * A <code>VolcanoCluster</code> is a collection of {@link SaffronRel}ational
 * expressions which have the same environment.
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
public class VolcanoCluster
{
    //~ Instance fields -------------------------------------------------------

    public final Environment env;
    public final SaffronTypeFactory typeFactory;
    public final VolcanoQuery query;
    final VolcanoPlanner planner;
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
    VolcanoCluster(
        VolcanoQuery query,
        Environment env,
        VolcanoPlanner planner,
        SaffronTypeFactory typeFactory,
        RexBuilder rexBuilder)
    {
        assert(planner != null);
        assert(typeFactory != null);
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

    public SaffronPlanner getPlanner()
    {
        return planner;
    }
}


// End VolcanoCluster.java
