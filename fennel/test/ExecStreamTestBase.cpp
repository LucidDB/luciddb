/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/ExecStreamTestBase.h"
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/DfsTreeExecStreamScheduler.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void ExecStreamTestBase::prepareGraphTwoStreams(
    SharedExecStream pStream1,
    SharedExecStream pStream2)
{
    pGraph->addStream(pStream1);
    pGraph->addStream(pStream2);
    pGraph->addDataflow(
        pStream1->getStreamId(),
        pStream2->getStreamId());
    pGraph->addOutputDataflow(
        pStream2->getStreamId());
    pGraph->prepare(*pScheduler);
}

void ExecStreamTestBase::testCaseSetUp()
{
    SegStorageTestBase::testCaseSetUp();
    openStorage(DeviceMode::createNew);
    pGraph.reset(new ExecStreamGraphImpl(),ClosableObjectDestructor());
    pScheduler.reset(new DfsTreeExecStreamScheduler(pGraph));
}

void ExecStreamTestBase::testCaseTearDown()
{
    if (pScheduler) {
        pScheduler->stop();
    }
    pGraph.reset();
    pScheduler.reset();
    SegStorageTestBase::testCaseTearDown();
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamTestBase.cpp
