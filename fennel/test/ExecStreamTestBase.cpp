/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/ExecStreamTestBase.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStreamGraphEmbryo.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/DfsTreeExecStreamScheduler.h"
#include "fennel/exec/SimpleExecStreamGovernor.h"

#include <boost/test/test_tools.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

SharedExecStreamGraph ExecStreamTestBase::newStreamGraph()
{
    return ExecStreamGraph::newExecStreamGraph();
}

SharedExecStreamGraphEmbryo
ExecStreamTestBase::newStreamGraphEmbryo(SharedExecStreamGraph g)
{
    return SharedExecStreamGraphEmbryo(
        new ExecStreamGraphEmbryo(
            g, pScheduler, pCache, pSegmentFactory));
}

ExecStreamScheduler *ExecStreamTestBase::newScheduler()
{
    return new DfsTreeExecStreamScheduler(
        shared_from_this(),
        "DfsTreeExecStreamScheduler");
}

ExecStreamGovernor *ExecStreamTestBase::newResourceGovernor(
    ExecStreamResourceKnobs const &knobSettings,
    ExecStreamResourceQuantity const &resourcesAvailable)
{
    return new SimpleExecStreamGovernor(
        knobSettings, resourcesAvailable, shared_from_this(),
        "SimpleExecStreamGovernor");
}

void ExecStreamTestBase::testCaseSetUp()
{
    SegStorageTestBase::testCaseSetUp();
    openStorage(DeviceMode::createNew);
    pScheduler.reset(newScheduler());
    ExecStreamResourceKnobs knobSettings;
    knobSettings.cacheReservePercentage = DefaultCacheReservePercent;
    knobSettings.expectedConcurrentStatements = DefaultConcurrentStatements;
    ExecStreamResourceQuantity resourcesAvailable;
    resourcesAvailable.nCachePages = nMemPages;
    pResourceGovernor.reset(
        newResourceGovernor(knobSettings, resourcesAvailable));
}

void ExecStreamTestBase::testCaseTearDown()
{
    // first stop the scheduler
    if (pScheduler) {
        pScheduler->stop();
    }
    // destroy the graph
    tearDownExecStreamTest();
    // free the scheduler last, since an ExecStreamGraph holds a raw Scheduler
    // ptr
    pScheduler.reset(); 
    assert(pResourceGovernor.unique());
    pResourceGovernor.reset();
    SegStorageTestBase::testCaseTearDown();
}

void ExecStreamTestBase::tearDownExecStreamTest()
{
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamTestBase.cpp
