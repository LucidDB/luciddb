/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

import openjava.ptree.ParseTree;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.CallingConvention;


/**
 * A relational expression of one of the the Java-based calling conventions.
 *
 * <p>Objects which implement this interface must:<ul>
 *
 * <li>extend {@link org.eigenbase.rel.AbstractRelNode}, and</li>
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
public interface JavaRel extends RelNode
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Creates a plan for this expression according to a calling convention.
     *
     * @param implementor implementor
     */
    ParseTree implement(JavaRelImplementor implementor);
}


// End JavaRel.java
