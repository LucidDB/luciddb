/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
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

package net.sf.saffron.core;

import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;


/**
 * An <code>ImplementableTable</code> is a {@link RelOptTable} which can be
 * scanned without any filters. For example, the tables of all
 * <code>int</code>s or all Java classes cannot, but any {@link
 * net.sf.saffron.ext.JdbcTable} can (using <code>select &#42; from
 * <i>table</i></code>).
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 8 February, 2002
 */
public interface ImplementableTable extends RelOptTable
{
    void implement(
        RelNode rel,
        JavaRelImplementor implementor);
}


// End ImplementableTable.java
