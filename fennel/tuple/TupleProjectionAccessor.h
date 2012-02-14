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

#ifndef Fennel_TupleProjectionAccessor_Included
#define Fennel_TupleProjectionAccessor_Included

#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

class TupleAccessor;
class AttributeAccessor;
class TupleProjection;

/**
 * A TupleProjectionAccessor provides a way to efficiently unmarshal
 * selected attributes of a tuple, as explained in
 * the <a href="structTupleDesign.html#TupleProjection">design docs</a>.
 */
class FENNEL_TUPLE_EXPORT TupleProjectionAccessor
{
    TupleAccessor const *pTupleAccessor;
    std::vector<AttributeAccessor const *> ppAttributeAccessors;

public:
    explicit TupleProjectionAccessor();

    void bind(
        TupleAccessor const &tupleAccessor,
        TupleProjection const &tupleProjection);

    virtual ~TupleProjectionAccessor();

    void unmarshal(TupleData &tuple) const
    {
        unmarshal(tuple.begin());
    }

    void unmarshal(TupleData::iterator tupleIter) const;

    uint size() const
    {
        return ppAttributeAccessors.size();
    }
};

FENNEL_END_NAMESPACE

#endif

// End TupleProjectionAccessor.h
