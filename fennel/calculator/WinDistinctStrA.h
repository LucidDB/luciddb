/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2009 Dynamo BI Corporation
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
//
// WinAggHistogram - Windowed Aggregation Histogram object.
*/

#ifndef Fennel_WinDistinctStrA_Included
#define Fennel_WinDistinctStrA_Included

#include "WinDistinctBase.h"
#include "WinAggHistogramStrA.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/calculator/CalcCommon.h"
#include "fennel/common/TraceSource.h"

#include <utility>
#include <boost/unordered_map.hpp>
//#include <map>

FENNEL_BEGIN_NAMESPACE

/**
 * Support structure for calculating various windowed aggregation
 * functions (COUNT, SUM, AVG, MIN, MAX, FIRST_VALUE, LAST_VALUE).
 *
 * Each row entry is held in
 * a tree structure to make finding new min/max functions easy as values
 * are added/removed from the window.  Running sum is also kept up to date
 * as rows enter and exit the window.
 *
 * It is provided as a parameter to all windowed agg support functions.
 *
 * @author JFrost
 * @version $Id$
 */

class FENNEL_CALCULATOR_EXPORT WinDistinctStrA : WinDistinctBase
{

public:
    WinDistinctStrA()
        : WinDistinctBase(),
          currentWindow()
    {}

    ~WinDistinctStrA()
    {}

    typedef boost::unordered_map<
            StringDesc, int64_t,
            WinAggHistogramStrA::StringDescHash,
            WinAggHistogramStrA::StringDescEquals> WinDistinctData;

//    typedef std::map<
//            StringDesc, int64_t,
//            WinAggHistogramStrA::StringDescCompare> WinDistinctData;

    //! addRow - Adds new value to tree and updates
    //! the running sum for current values.
    //!
    //! Input - New value to be added to the tree
    //
    void addRow(RegisterRef<char *>* node);

    //! dropRow - Removes a value from the tree and updates
    //! the running sum.
    //!
    //! Input - Value to be removed from the tree
    //
    void dropRow(RegisterRef<char *>* node);

private:

    // Holds the values currently in the window.
    WinDistinctData currentWindow;
};

FENNEL_END_NAMESPACE

#endif

// End WinDistinctStrA.h
