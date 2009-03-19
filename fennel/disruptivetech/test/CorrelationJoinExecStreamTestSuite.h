/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 SQLstream, Inc.
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

#ifndef Fennel_DT_Test_CorrelationJoinExecStreamTestSuite_Included
#define Fennel_DT_Test_CorrelationJoinExecStreamTestSuite_Included

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/ExecStreamUnitTestBase.h"
#include <boost/test/test_tools.hpp>
using namespace fennel;

class CorrelationJoinExecStreamTestSuite : public ExecStreamUnitTestBase
{
    TupleAttributeDescriptor descAttrInt64;
    TupleDescriptor descInt64;
    TupleAttributeDescriptor descAttrVarbinary16;
    TupleDescriptor descVarbinary16;

public:
    explicit CorrelationJoinExecStreamTestSuite(bool addAllTests = true);
    void testCorrelationJoin();
};

#endif
