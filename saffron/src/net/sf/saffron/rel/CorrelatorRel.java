/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

package net.sf.saffron.rel;

import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.util.Util;
import net.sf.saffron.rex.RexLiteral;
import openjava.ptree.Expression;

import java.util.Collections;


/**
 * A <code>CorrelatorRel</code> behaves like a kind of {@link JoinRel}, but
 * works by setting variables in its environment and restarting its
 * right-hand input.  It is used to represent a correlated query; one
 * implementation option is to de-correlate the expression.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 23 September, 2001
 */
public class CorrelatorRel extends JoinRel
{
    //~ Instance fields -------------------------------------------------------

    Expression [] correlations;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a Correlator.
     *
     * @param cluster {@link VolcanoCluster} this relational expression
     *        belongs to
     * @param left left input relational expression
     * @param right right  input relational expression
     * @param joinType join type (see {@link JoinRel#joinType})
     * @param correlations set of expressions to set as variables each time a
     *        row arrives from the left input
     */
    CorrelatorRel(
        VolcanoCluster cluster,
        SaffronRel left,
        SaffronRel right,
        int joinType,
        Expression [] correlations)
    {
        super(
            cluster,
            left,
            right,
                cluster.rexBuilder.makeLiteral(true),
            joinType,
            Collections.EMPTY_SET);
        this.correlations = correlations;
    }

    //~ Methods ---------------------------------------------------------------

    public Object clone()
    {
        return new CorrelatorRel(
            cluster,
            (SaffronRel) left.clone(),
            (SaffronRel) right.clone(),
            joinType,
            Util.clone(correlations));
    }
}


// End CorrelatorRel.java
