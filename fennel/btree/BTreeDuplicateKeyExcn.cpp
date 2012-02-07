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

#include "fennel/common/CommonPreamble.h"
#include "fennel/btree/BTreeDuplicateKeyExcn.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/common/FennelResource.h"

#include <sstream>

FENNEL_BEGIN_CPPFILE("$Id$");

BTreeDuplicateKeyExcn::BTreeDuplicateKeyExcn(
    TupleDescriptor const &keyDescriptor,
    TupleData const &keyData)
    : FennelExcn("")
{
    std::ostringstream oss;
    // TODO:  nicer formatting
    TuplePrinter tuplePrinter;
    tuplePrinter.print(oss, keyDescriptor, keyData);
    msg = oss.str();
    msg = FennelResource::instance().duplicateKeyDetected(msg);
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeDuplicateKeyExcn.cpp
