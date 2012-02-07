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

#ifndef Fennel_LogicalTxnParticipantFactory_Included
#define Fennel_LogicalTxnParticipantFactory_Included

FENNEL_BEGIN_NAMESPACE

class LogicalTxnParticipant;

/**
 * LogicalTxnParticipantFactory defines an interface for reconstructing
 * instances of LogicalTxnParticipant during recovery.
 */
class FENNEL_TXN_EXPORT LogicalTxnParticipantFactory
{
public:
    virtual ~LogicalTxnParticipantFactory();

    /**
     * Recovers a LogicalTxnParticipant from the log.  Using the classId to
     * determine the participant type to create, the factory reads required
     * constructor parameters from the log input stream.  The factory may peool
     * participant instances; i.e. when the same constructor parameters are
     * encountered a second time, the factory can return the same instance.
     * (TODO:  refine this when parallelized recovery is implemented.)  The
     * implementation must consume ALL log data for this record, even if some
     * of it turns out to be unneeded.
     *
     * @param classId the LogicalTxnClassId recorded when the participant was
     * logged while online
     *
     * @param logStream the log information written by the participant's
     * describeParticipant() implementation
     *
     * @return reference to loaded participant
     */
    virtual SharedLogicalTxnParticipant loadParticipant(
        LogicalTxnClassId classId,
        ByteInputStream &logStream) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End LogicalTxnParticipantFactory.h
