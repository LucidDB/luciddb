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

#ifndef Fennel_FileStatsTarget_Included
#define Fennel_FileStatsTarget_Included

#include "fennel/common/StatsTarget.h"

#include <fstream>

FENNEL_BEGIN_NAMESPACE

/**
 * FileStatsTarget implements the StatsTarget interface by writing to a simple
 * text file.
 */
class FENNEL_COMMON_EXPORT FileStatsTarget : public StatsTarget
{
    std::string filename;
    std::ofstream snapshotStream;

public:
    /**
     * Creates a new FileStatsTarget.
     *
     * @param filename name of file into which to write stats
     */
    explicit FileStatsTarget(std::string filename);

    /**
     * Gets name of file receiving stats.
     */
    std::string getFilename() const;

    // implement the StatsTarget interface
    virtual void beginSnapshot();
    virtual void endSnapshot();
    virtual void writeCounter(std::string name, int64_t value);
};

FENNEL_END_NAMESPACE

#endif

// End FileStatsTarget.h
