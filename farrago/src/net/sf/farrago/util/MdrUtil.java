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
package net.sf.farrago.util;

import java.util.*;
import java.util.logging.*;

import org.eigenbase.enki.mdr.*;
import org.eigenbase.enki.netbeans.*;


// NOTE:  This class gets compiled independently of everything else since
// it is used by build-time utilities such as ProxyGen.  That means it must
// have no dependencies on other Farrago code.

/**
 * Static MDR utilities.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MdrUtil
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Loads an MDRepository instance.
     *
     * @param storageFactoryClassName fully qualified name of the class used to
     * implement repository storage (if null, this defaults to BtreeFactory if
     * not specified as an entry in storageProps)
     * @param storageProps storage-specific properties (with or without the
     * MDRStorageProperty prefix)
     *
     * @return loaded repository
     */
    public static EnkiMDRepository loadRepository(
        String storageFactoryClassName,
        Properties storageProps)
    {
        String classNameProp =
            "org.netbeans.mdr.storagemodel.StorageFactoryClassName";

        if (storageFactoryClassName != null) {
            storageProps.put(classNameProp, storageFactoryClassName);
        }

        EnkiMDRepository repos =
            MDRepositoryFactory.newMDRepository(storageProps);

        return repos;
    }

    /**
     * Integrates MDR tracing with Farrago tracing. Must be called before first
     * usage of MDR.
     *
     * @param mdrTracer Logger for MDR tracing
     */
    public static void integrateTracing(Logger mdrTracer)
    {
        MdrTraceUtil.integrateTracing(mdrTracer);
    }
}

// End MdrUtil.java
