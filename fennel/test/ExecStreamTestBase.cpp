/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
            g, pScheduler, pCache, pSegmentFactory, true));
}

ExecStreamScheduler *ExecStreamTestBase::newScheduler()
{
    return new DfsTreeExecStreamScheduler(
        this->shared_from_this(),
        "DfsTreeExecStreamScheduler");
}

void ExecStreamTestBase::testCaseSetUp()
{
    SegStorageTestBase::testCaseSetUp();
    openStorage(DeviceMode::createNew);
    pScheduler.reset(newScheduler());
}

void ExecStreamTestBase::testCaseTearDown()
{
    // first stop the scheduler
    if (pScheduler) {
        pScheduler->stop();
    }
    tearDown();
    // free the scheduler last, since an ExecStreamGraph hold a raw Scheduler ptr
    pScheduler.reset(); 
    SegStorageTestBase::testCaseTearDown();
}

void ExecStreamTestBase::tearDown()
{
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamTestBase.cpp
