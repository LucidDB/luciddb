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

package net.sf.saffron.runtime;

import net.sf.saffron.core.AggregationExtender;


/**
 * <code>AggAndAcc</code> holds an instance of an {@link AggregationExtender}
 * and its associated accumulator.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 18 February, 2002
 */
public class AggAndAcc
{
    public AggregationExtender agg;
    public Object total;

    public AggAndAcc(
        AggregationExtender agg,
        Object total)
    {
        this.agg = agg;
        this.total = total;
    }
}


// End AggAndAcc.java
