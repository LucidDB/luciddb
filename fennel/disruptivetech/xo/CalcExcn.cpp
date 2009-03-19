/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2003-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
// Portions Copyright (C) 1999-2009 John V. Sichi
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
#include "fennel/disruptivetech/xo/CalcExcn.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/common/FennelResource.h"

#include <sstream>

FENNEL_BEGIN_CPPFILE("$Id$");

CalcExcn::CalcExcn(
    std::string warnings,
    TupleDescriptor const &inputDescriptor,
    TupleData const &inputData)
    : FennelExcn("")
{
    // TODO: use fennel resouce and map sqlState to specific messages
    std::ostringstream oss;
    oss << "could not calculate results for the following row:" << std::endl;
    // TODO:  nicer formatting
    TuplePrinter tuplePrinter;
    tuplePrinter.print(oss,inputDescriptor,inputData);
    oss << std::endl << "Messages:" << std::endl << warnings;
    msg = oss.str();
}

FENNEL_END_CPPFILE("$Id$");

// End CalcExcn.cpp
