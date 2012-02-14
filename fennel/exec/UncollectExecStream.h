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

#ifndef Fennel_UncollectExecStream_Included
#define Fennel_UncollectExecStream_Included

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * UncollectExecStreamParams defines parameters for instantiating a
 * UncollectExecStream.
 */
struct FENNEL_EXEC_EXPORT UncollectExecStreamParams
    : public ConduitExecStreamParams
{
    //empty
};

/**
 * Ouputs all tuples that previously has been collected by CollectExecStream
 *
 * @author Wael Chatila
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT UncollectExecStream
    : public ConduitExecStream
{
private:
    TupleData inputTupleData;
    TupleData outputTupleData;
    uint      bytesWritten;
public:
    virtual void prepare(UncollectExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End UncollectExecStream.h
