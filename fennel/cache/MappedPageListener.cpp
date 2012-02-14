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
#include "fennel/cache/MappedPageListener.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void MappedPageListener::notifyPageMap(CachePage &)
{
}

void MappedPageListener::notifyPageUnmap(CachePage &)
{
}

void MappedPageListener::notifyAfterPageRead(CachePage &)
{
}

void MappedPageListener::notifyPageDirty(CachePage &,bool)
{
}

void MappedPageListener::notifyBeforePageFlush(CachePage &)
{
}

void MappedPageListener::notifyAfterPageFlush(CachePage &)
{
}

bool MappedPageListener::canFlushPage(CachePage &)
{
    return true;
}

MappedPageListener::~MappedPageListener()
{
}

MappedPageListener *MappedPageListener::notifyAfterPageCheckpointFlush(
    CachePage &page)
{
    return NULL;
}

FENNEL_END_CPPFILE("$Id$");

// End MappedPageListener.cpp
