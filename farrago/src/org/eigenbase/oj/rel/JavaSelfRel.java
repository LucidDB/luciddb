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

package org.eigenbase.oj.rel;

import openjava.ptree.Expression;


/**
 * A relational expression which implements this interface can generate a java
 * expression which represents the current row of the expression.
 *
 * @author jhyde
 * @since May 24, 2004
 * @version $Id$
 **/
public interface JavaSelfRel extends JavaRel
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Returns a Java expression which yields the current row of this
     * relational expression. This method is called by the {@link
     * JavaRelImplementor} the first time a piece of Java code wants to refer to
     * this relation. The implementor then uses this expression to initialize
     * a variable.
     *
     * <p>
     * If no code needs to refer to this relation, then the expression is
     * never generated. This prevents generating useless code like
     * <blockquote>
     * <pre>Dummy_12f614.Ojp_1 oj_var8 = new Dummy_12f614.Ojp_1();</pre>
     * </blockquote>
     * .
     * </p>
     *
     * <p>
     * If a relational expression has one input relational expression
     * which has the same row type, you may be able to share its variable.
     * Call Implementor#bind(Rel,Rel) to do this.
     *
     * @see JavaRelImplementor#bind(org.eigenbase.rel.RelNode,org.eigenbase.rel.RelNode)
     */
    Expression implementSelf(JavaRelImplementor implementor);
}


// End JavaSelfRel.java
