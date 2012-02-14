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

#ifndef Fennel_CopyExecStream_Included
#define Fennel_CopyExecStream_Included

#include "fennel/exec/ConduitExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * CopyExecStreamParams defines parameters for CopyExecStream.
 */
struct FENNEL_EXEC_EXPORT CopyExecStreamParams
    : public ConduitExecStreamParams
{
};

/**
 * CopyExecStream is an adapter for converting the output of a BUFPROV_PRODUCER
 * producer stream for use by a BUFPROF_CONSUMER consumer stream.
 * The implementation copies tuples from the producer buffer to the consumer
 * buffer.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT CopyExecStream
    : public ConduitExecStream
{
public:
    // implement ExecStream
    virtual void prepare(CopyExecStreamParams const &params);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End CopyExecStream.h
