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

package net.sf.saffron.oj.xlat;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
import org.eigenbase.util.Util;


/**
 * Converts references to agg items and aggregates (created by {@link
 * AggInternalTranslator}) into more conventional field accesses.
 */
class AggUnpickler
{
    private final RexBuilder rexBuilder;
    int groupCount;

    AggUnpickler(
        RexBuilder rexBuilder,
        int groupCount)
    {
        this.rexBuilder = rexBuilder;
        this.groupCount = groupCount;
    }
}


// End AggUnpickler.java
