/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
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
#include "fennel/test/SegmentTestBase.h"

using namespace fennel;

class LinearDeviceSegmentTest : virtual public SegmentTestBase
{
public:
    explicit LinearDeviceSegmentTest()
    {
        FENNEL_UNIT_TEST_CASE(SegmentTestBase,testSingleThread);
        FENNEL_UNIT_TEST_CASE(PagingTestBase,testMultipleThreads);
    }
};

FENNEL_UNIT_TEST_SUITE(LinearDeviceSegmentTest);

// End LinearDeviceSegmentTest.cpp
