/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.oj.rel;

/**
 * A relational expression which implements itself by generating Java
 * flow-control statements. This interface corresponds to the {@link
 * org.eigenbase.relopt.CallingConvention#JAVA Java calling-convention}.
 *
 * <p>For example, {@link JavaFilterRel} implements filtering logic by
 * generating an <code>if (...) { ... }</code> construct.
 *
 * @author jhyde
 * @version $Id$
 * @since May 27, 2004
 */
public interface JavaLoopRel
    extends JavaRel
{

    //~ Methods ----------------------------------------------------------------

    /**
     * Calls a parent back.
     *
     * @param implementor implementor
     * @param ordinal of the child which is making the call
     */
    void implementJavaParent(
        JavaRelImplementor implementor,
        int ordinal);
}

// End JavaLoopRel.java
