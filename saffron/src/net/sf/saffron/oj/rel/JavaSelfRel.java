/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2004-2004 Disruptive Tech
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
package net.sf.saffron.oj.rel;

import openjava.ptree.Expression;

/**
 * A relational expression which implements this interface can generate a java
 * expression which represents the current row of the expression.
 *
 * @author jhyde
 * @since May 24, 2004
 * @version $Id$
 **/
public interface JavaSelfRel extends JavaRel {
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
     * @see JavaRelImplementor#bind(net.sf.saffron.rel.SaffronRel,net.sf.saffron.rel.SaffronRel)
     */
    Expression implementSelf(JavaRelImplementor implementor);

}

// End JavaSelfRel.java
