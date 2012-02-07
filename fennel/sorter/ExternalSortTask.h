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

#ifndef Fennel_ExternalSortTask_Included
#define Fennel_ExternalSortTask_Included

FENNEL_BEGIN_NAMESPACE

class ExternalSortExecStreamImpl;
class ExternalSortRunLoader;

/**
 * ExternalSortTask represents a task entry in the queue serviced by
 * the parallel sorter's thread pool.  Currently, only the run-generation phase
 * is parallelized.  After each run is loaded by the main thread, it is
 * dispatched as a task to be sorted and stored by a thread from the pool.
 */
class FENNEL_SORTER_EXPORT ExternalSortTask
{
    /**
     * The stream on behalf of which this task is working.
     */
    ExternalSortExecStreamImpl &sortStream;

    /**
     * The pre-loaded run to be sorted and stored by this task.
     */
    ExternalSortRunLoader &runLoader;

public:
    explicit ExternalSortTask(
        ExternalSortExecStreamImpl &sortStreamInit,
        ExternalSortRunLoader &runLoaderInit)
        : sortStream(sortStreamInit),
          runLoader(runLoaderInit)
    {
    }

    /**
     * Executes this request; this satisfies the ThreadPool Task signature,
     * allowing instances of this class to be submitted as a Task to
     * ThreadPoolScheduler.
     */
    void execute();
};

FENNEL_END_NAMESPACE

#endif

// End ExternalSortTask.h
