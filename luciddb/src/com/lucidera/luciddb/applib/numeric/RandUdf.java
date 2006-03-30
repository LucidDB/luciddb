/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.luciddb.applib.numeric;

import java.util.Random;
import java.sql.Types;
import com.lucidera.luciddb.applib.resource.*;
import net.sf.farrago.runtime.*;

/**
 * rand returns a random integer given a range of numbers.
 *
 * Ported from //BB/bb713/server/SQL/rand.java
 */
public class RandUdf
{

    public static int execute( int minVal, int maxVal ) throws ApplibException
    {
        if( maxVal < minVal ) {
            throw ApplibResourceObject.get().MinNotSmallerThanMax.ex();
        }

        Random oRand = (Random)FarragoUdrRuntime.getContext();
        if (oRand == null) {
            oRand = new Random();
        }

        // Generate a double precision number between 0 and 1
        double randDbl = oRand.nextDouble();

        // Now scale that number to the range minVal and maxVal
        int randInt = minVal +
            (int) Math.round( randDbl * (double) (maxVal - minVal) );

        return randInt;
    }
}

// End RandUdf.java
