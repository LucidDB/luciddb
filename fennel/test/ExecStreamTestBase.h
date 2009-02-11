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

#ifndef Fennel_ExecStreamTestBase_Included
#define Fennel_ExecStreamTestBase_Included

#include "fennel/exec/ExecStreamGovernor.h"
#include "fennel/test/SegStorageTestBase.h"

FENNEL_BEGIN_NAMESPACE

class ExecStreamScheduler;

/**
 * ExecStreamTestBase is a common base for tests of ExecStream
 * implementations.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class ExecStreamTestBase : virtual public SegStorageTestBase
{
protected:
    static const uint DefaultCacheReservePercent = 5;

    static const uint DefaultConcurrentStatements = 4;

    SharedExecStreamScheduler pScheduler;

    SharedExecStreamGovernor pResourceGovernor;

    SharedCacheAccessor pCacheAccessor;

    /**
     * Creates a stream graph.
     */
    virtual SharedExecStreamGraph newStreamGraph();

    /**
     * Creates an embryo for a stream graph.
     */
    virtual SharedExecStreamGraphEmbryo newStreamGraphEmbryo(
        SharedExecStreamGraph);

    /**
     * Creates a scheduler.
     */
    virtual ExecStreamScheduler *newScheduler();

    /**
     * Creates the resource governor
     */
    virtual ExecStreamGovernor *newResourceGovernor(
        ExecStreamResourceKnobs const &knobSettings,
        ExecStreamResourceQuantity const &resourcesAvailable);

    /**
     * ExecStream-specific handler called from testCaseTearDown.
     */
    virtual void tearDownExecStreamTest();

public:
    virtual ~ExecStreamTestBase() {}

    // override TestBase
    virtual void testCaseSetUp();
    virtual void testCaseTearDown();
};

FENNEL_END_NAMESPACE
#endif
// End ExecStreamTestBase.h
