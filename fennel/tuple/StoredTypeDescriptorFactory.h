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

#ifndef Fennel_StoredTypeDescriptorFactory_Included
#define Fennel_StoredTypeDescriptorFactory_Included

FENNEL_BEGIN_NAMESPACE

class StoredTypeDescriptor;

/**
 * StoredTypeDescriptorFactory is an abstract factory defining how
 * StoredTypeDescriptors are instantiated, as described in
 * <a href="structTupleDesign.html#StoredTypeDescriptor">the design docs</a>.
 */
class FENNEL_TUPLE_EXPORT StoredTypeDescriptorFactory
{
public:
    virtual ~StoredTypeDescriptorFactory();

    /**
     * Instantiates a StoredTypeDescriptor.
     *
     *<p>
     *
     * TODO:  extend this to cover type parameters such as precision and scale?
     *
     * @param iTypeOrdinal the ordinal for the type
     *
     * @return the corresponding data type object
     */
    virtual StoredTypeDescriptor const &newDataType(
        StoredTypeDescriptor::Ordinal iTypeOrdinal) const = 0;
};

FENNEL_END_NAMESPACE

#endif

// End StoredTypeDescriptorFactory.h
