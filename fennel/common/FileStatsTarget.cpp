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
#include "fennel/common/FileStatsTarget.h"
#include "fennel/common/StatsSource.h"

FENNEL_BEGIN_CPPFILE("$Id$");

FileStatsTarget::FileStatsTarget(std::string filenameInit)
{
    filename = filenameInit;
}

std::string FileStatsTarget::getFilename() const
{
    return filename;
}

void FileStatsTarget::beginSnapshot()
{
    assert(!filename.empty());
    snapshotStream.open(filename.c_str(), std::ios::trunc);

    // TODO:  re-enable this.  I disabled it since /tmp/fennel.stats
    // can't be opened on mingw; need to parameterize it better
    // (or put in Performance Monitor integration)

    // assert(snapshotStream.good());
}

void FileStatsTarget::endSnapshot()
{
    snapshotStream.close();
}

void FileStatsTarget::writeCounter(std::string name, int64_t value)
{
    snapshotStream << name << ' ' << value << std::endl;
}

StatsSource::~StatsSource()
{
}

StatsTarget::~StatsTarget()
{
}

FENNEL_END_CPPFILE("$Id$");

// End FileStatsTarget.cpp
