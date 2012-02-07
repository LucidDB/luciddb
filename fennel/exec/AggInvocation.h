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

#ifndef Fennel_AggInvocation_Included
#define Fennel_AggInvocation_Included

#include "fennel/common/FemEnums.h"

FENNEL_BEGIN_NAMESPACE

/**
 * AggInvocation represents one call to an aggregate function.
 *
 * @author John V. Sichi
 * @version $Id$
 */
struct FENNEL_EXEC_EXPORT AggInvocation
{
    /**
     * Aggregate function to be computed.
     */
    AggFunction aggFunction;

    // TODO jvs 6-Oct-2005: May need to generalize this to a
    // TupleProjection, since I think SQL:2003 allows SUM(A,B), producing
    // a ROW result value; need to check.
    /**
     * 0-based index of source attribute in input tuple, or -1 for none,
     * as in the case of COUNT(*).
     */
    int iInputAttr;
};

typedef std::vector<AggInvocation> AggInvocationList;
typedef std::vector<AggInvocation>::iterator AggInvocationIter;
typedef std::vector<AggInvocation>::const_iterator AggInvocationConstIter;

FENNEL_END_NAMESPACE

#endif

// End AggInvocation.h
