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

#ifndef Fennel_BTreeDuplicateKeyExcn_Included
#define Fennel_BTreeDuplicateKeyExcn_Included

#include "fennel/common/FennelExcn.h"

FENNEL_BEGIN_NAMESPACE

class TupleDescriptor;
class TupleData;

/**
 * Exception class for duplicate keys encountered during insert or update.
 */
class FENNEL_BTREE_EXPORT BTreeDuplicateKeyExcn
    : public FennelExcn
{
public:
    /**
     * Constructs a new BTreeDuplicateKeyExcn.
     *
     *<p>
     *
     * TODO:  take more information to get a better key description
     *
     * @param keyDescriptor TupleDescriptor for the BTree's key
     *
     * @param keyData data for the duplicate key
     */
    explicit BTreeDuplicateKeyExcn(
        TupleDescriptor const &keyDescriptor,
        TupleData const &keyData);
};

FENNEL_END_NAMESPACE

#endif

// End BTreeDuplicateKeyExcn.h
