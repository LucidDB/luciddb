/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
