/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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
#include "fennel/test/TestBase.h"
#include "fennel/common/FennelResource.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

class ResourceTest : virtual public TestBase
{
public:
    
    explicit ResourceTest()
    {
        FENNEL_UNIT_TEST_CASE(ResourceTest,testEnUsLocale);
    }
    
    void testEnUsLocale();
};

void ResourceTest::testEnUsLocale()
{
    Locale locale("en","US");
    std::string actual =
        FennelResource::instance(locale).sysCallFailed("swizzle");
    std::string expected = "System call failed:  swizzle";
    BOOST_CHECK_EQUAL(expected,actual);
}

FENNEL_UNIT_TEST_SUITE(ResourceTest);

// End ResourceTest.cpp

