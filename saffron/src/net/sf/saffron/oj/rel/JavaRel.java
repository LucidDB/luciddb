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

import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.opt.CallingConvention;
import openjava.ptree.ParseTree;

/**
 * A relational expression of one of the the Java-based calling conventions.
 *
 * <p>Objects which implement this interface must:<ul>
 *
 * <li>extend {@link net.sf.saffron.rel.SaffronBaseRel}, and</li>
 *
 * <li>return one of the following calling-conventions from their
 * {@link #getConvention} method:
 * {@link CallingConvention#ARRAY ARRAY},
 * {@link CallingConvention#ITERABLE ITERABLE},
 * {@link CallingConvention#ITERATOR ITERATOR},
 * {@link CallingConvention#COLLECTION COLLECTION},
 * {@link CallingConvention#MAP MAP},
 * {@link CallingConvention#VECTOR VECTOR},
 * {@link CallingConvention#HASHTABLE HASHTABLE},
 * {@link CallingConvention#JAVA JAVA}.</li></ul>
 *
 * <p>For {@link CallingConvention#JAVA JAVA calling-convention}, see
 * the sub-interface {@link JavaLoopRel}, and the auxilliary interface
 * {@link JavaSelfRel}.
 *
 * @author jhyde
 * @since Nov 22, 2003
 * @version $Id$
 **/
public interface JavaRel extends SaffronRel {
    /**
     * Creates a plan for this expression according to a calling convention.
     *
     * @param implementor implementor
     */
    ParseTree implement(JavaRelImplementor implementor);
}

// End JavaRel.java
