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
package net.sf.farrago.catalog;

import java.util.*;

import org.netbeans.mdr.persistence.*;
import org.netbeans.mdr.persistence.memoryimpl.*;


/**
 * Factory for {@link FarragoTransientStorage}. Adapted from
 * org.netbeans.mdr.persistence.memoryimpl.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoTransientStorageFactory
    implements StorageFactory
{
    //~ Static fields/initializers ---------------------------------------------

    // distinguish this from normal memory storage
    static final String NULL_STORAGE_ID = "#";

    private static final MOFID NULL_MOFID = new MOFID(0, NULL_STORAGE_ID);
    private static FarragoTransientStorage singletonStorage;

    //~ Constructors -----------------------------------------------------------

    public FarragoTransientStorageFactory()
    {
    }

    //~ Methods ----------------------------------------------------------------

    // implement StorageFactory
    public synchronized Storage createStorage(Map properties)
        throws StorageException
    {
        singletonStorage = new FarragoTransientStorage();
        return singletonStorage;
    }

    // implement StorageFactory
    public MOFID createNullMOFID()
        throws StorageException
    {
        return NULL_MOFID;
    }
}

// End FarragoTransientStorageFactory.java
