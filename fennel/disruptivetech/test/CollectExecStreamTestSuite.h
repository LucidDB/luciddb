/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 The Eigenbase Project
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

#ifndef Fennel_DT_Test_CollectExecStreamTestSuite_Included
#define Fennel_DT_Test_CollectExecStreamTestSuite_Included

#include "fennel/test/ExecStreamUnitTestBase.h"
#include <boost/test/test_tools.hpp>
using namespace fennel;

/**
 * Test Suite for the collect/uncollect xo's
 * @author Wael Chatila
 */
class CollectExecStreamTestSuite : public ExecStreamUnitTestBase
{
    TupleAttributeDescriptor descAttrInt64;
    TupleDescriptor descInt64;
    TupleAttributeDescriptor descAttrVarbinary32;
    TupleDescriptor descVarbinary32;

public:
    explicit CollectExecStreamTestSuite(bool addAllTests = true);

    /**
     * Tests an stream input ints gets collected into an continues array
     */
    void testCollectInts();

    /**
     * Tests an stream going through a cascade of the collect and
     * the uncollect xos, expecting the same result back
     */
    void testCollectUncollect();

    /**
     * Tests an stream going through a cascade of two collect and
     * two uncollect xos, expecting the same result back
     */
    void testCollectCollectUncollectUncollect();


};

#endif
