/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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
package net.sf.farrago.query;

import net.sf.farrago.type.*;

import org.eigenbase.oj.util.*;
import org.eigenbase.rex.*;


/**
 * FarragoRexBuilder refines JavaRexBuilder with Farrago-specific details.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoRexBuilder extends JavaRexBuilder
{
    //~ Constructors ----------------------------------------------------------

    FarragoRexBuilder(FarragoTypeFactory typeFactory)
    {
        super(typeFactory);
    }

    //~ Methods ---------------------------------------------------------------

    // override JavaRexBuilder
    public RexLiteral makeLiteral(String s)
    {
        return makePreciseStringLiteral(s);
    }
}


// End FarragoRexBuilder.java
