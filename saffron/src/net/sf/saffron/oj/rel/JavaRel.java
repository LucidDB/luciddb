/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
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
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.opt.RelImplementor;

/**
 * A relational expression of the Java calling convention.
 *
 * <p>Objects which implement this interface must
 * (a) extend {@link SaffronRel}, and
 * (b) return {@link net.sf.saffron.opt.CallingConvention#JAVA} from their
 *     {@link SaffronRel#getConvention} method</p>
 *
 * @author jhyde$
 * @since Nov 22, 2003$
 * @version $Id$
 **/
public interface JavaRel {
    /**
     * Returns a Java expression which yields the current row of this
     * relational expression. This method is called by the {@link
     * RelImplementor} the first time a piece of Java code wants to refer to
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
     * @see net.sf.saffron.opt.RelImplementor#bind(SaffronRel,SaffronRel)
     */
    Expression implementSelf(RelImplementor implementor);
}

// End JavaRel.java
