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
package net.sf.farrago.rng;

import net.sf.farrago.ojrex.*;

import java.lang.reflect.*;

import org.eigenbase.sql.fun.*;
import org.eigenbase.util.*;

/**
 * FarragoRngImplementorTable extends {@link FarragoOJRexImplementorTable}
 * with code generation for the NEXT_RANDOM_INT operator.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRngImplementorTable extends FarragoOJRexImplementorTable
{
    private static FarragoRngImplementorTable instance;

    private FarragoRngImplementorTable(SqlStdOperatorTable opTab)
    {
        super(opTab);

        Method method;

        try {
            method = FarragoRngUDR.class.getMethod(
                "rng_next_int_internal",
                new Class [] {
                    Integer.TYPE,
                    String.class,
                    String.class
                });
        } catch (Exception ex) {
            throw Util.newInternal(ex);
        }
        registerOperator(
            FarragoRngOperatorTable.rngInstance().nextRandomInt,
            new FarragoOJRexStaticMethodImplementor(method, false, null));
    }

    /**
     * Retrieves the singleton, creating it if necessary.
     *
     * @return singleton with RNG-specific type
     */
    public static synchronized FarragoRngImplementorTable rngInstance()
    {
        if (instance == null) {
            instance = new FarragoRngImplementorTable(
                FarragoRngOperatorTable.rngInstance());
        }
        return instance;
    }
}

// End FarragoRngImplementorTable.java
