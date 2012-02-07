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

import net.sf.farrago.*;


/**
 * Mock implementation of {@link FarragoMetadataFactory}.
 *
 * @author Julian Hyde
 * @version $Id$
 */
public class MockFarragoMetadataFactory
    extends FarragoMetadataFactoryImpl
{
    //~ Constructors -----------------------------------------------------------

    public MockFarragoMetadataFactory()
    {
        super();
        MockMetadataFactory factoryImpl = new FactoryImpl();
        this.setRootPackage((FarragoPackage) factoryImpl.getRootPackage());
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class FactoryImpl
        extends MockMetadataFactory
    {
        protected RefPackageImpl newRootPackage()
        {
            return new RefPackageImpl(FarragoPackage.class);
        }
    }
}

// End MockFarragoMetadataFactory.java
