/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
*/
package org.eigenbase.applib.numeric;

import java.util.*;

import net.sf.farrago.runtime.*;

import org.eigenbase.applib.resource.*;


/**
 * rand returns a random integer given a range of numbers. Ported from
 * //BB/bb713/server/SQL/rand.java
 */
public class RandUdf
{
    //~ Methods ----------------------------------------------------------------

    public static int execute(int minVal, int maxVal)
        throws ApplibException
    {
        if (maxVal < minVal) {
            throw ApplibResource.instance().MinNotSmallerThanMax.ex();
        }

        Random oRand = (Random) FarragoUdrRuntime.getContext();
        if (oRand == null) {
            oRand = new Random();
            FarragoUdrRuntime.setContext(oRand);
        }

        // Generate a double precision number between 0 and 1
        double randDbl = oRand.nextDouble();

        // Now scale that number to the range minVal and maxVal
        int randInt =
            minVal
            + (int) Math.round(randDbl * (double) (maxVal - minVal));

        return randInt;
    }
}

// End RandUdf.java
