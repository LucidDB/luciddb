/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 The Eigenbase Project
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

#ifndef Fennel_WinAggHistogram_Included
#define Fennel_WinAggHistogram_Included

#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/disruptivetech/calc/CalcCommon.h"
#include "fennel/common/TraceSource.h"

#include <utility>
#include <set>

FENNEL_BEGIN_NAMESPACE

/**
 * This object provides the support structure for calculating various windowed
 * aggregation functions (count,sum,avg,min,max).  Each row entry is held in
 * a tree structure to make finding new min/max functions easy as values
 * are added/removed from the window.  Running sum is also kept up to date
 * as rows enter and exit the window.
 *
 * It is provided as a parameter to all windowed agg support functions.
 *
 * @author JFrost
 * @version $Id$
 */

template <typename STDTYPE>
class WinAggHistogram
{

public:
    WinAggHistogram():
        currentWindow(),
        nullRows(0),
        currentSum(0)
    {}

    ~WinAggHistogram()
    {}

    typedef multiset<STDTYPE> winAggData;


    //! addRow - Adds new value to tree and updates
    //! the running sum for current values.
    //!
    //! Input - New value to be added to the tree
    //
    void addRow(RegisterRef<STDTYPE>* node)
    {
        // Add the new node to the accumulator strucuture
        TupleDatum *pDatum = node->getBinding();

        if (!pDatum->isNull()) {
            STDTYPE val = node->value();
            (void) currentWindow.insert(val);
            currentSum += val;
        } else {
            ++nullRows;
        }
    }
    
    //! dropRow - Removes a value from the tree and updates
    //! the running sum..
    //!
    //! Input - Value to be removed from the tree
    //
    void dropRow(RegisterRef<STDTYPE>* node)
    {
        TupleDatum *pDatum = node->getBinding();

        if (!pDatum->isNull()) {
            assert(0 != currentWindow.size());
            STDTYPE* pData = node->refer();

            pair<typename winAggData::iterator, typename winAggData::iterator> entries =
                currentWindow.equal_range(*pData);

            assert(entries.first != entries.second);
            if (entries.first != entries.second) {
                currentWindow.erase(entries.first);
                currentSum -= *pData;
            }
        } else {
            assert(0 != nullRows);
            --nullRows;
        }
    }

    //! getMin - Returns the current MIN() value for the window.
    //!
    //! Returns NULL if the window is empty.
    void getMin(RegisterRef<STDTYPE>* node)
    {
        if (0 != currentWindow.size()) {
            node->value(*(currentWindow.begin()));
        } else {
            node->toNull();
        }
    }

    //! getMax - Returns the current MAX() value for the window.
    //!
    //! Returns NULL if the window is empty.
    void getMax(RegisterRef<STDTYPE>* node)
    {
        if (0 != currentWindow.size()) {
            node->value(*(--(currentWindow.end())));
        } else {
            node->toNull();
        }
    }

    //! getSum - return the current sum for all values in the window.
    //!
    //! Returns NULL if the window is empty.
    void getSum(RegisterRef<STDTYPE>* node)
    {
        if (0 != currentWindow.size()) {
            node->value(currentSum);
        } else {
            node->toNull();
        }
    }

    //! getCount - returns the current number of entries in the window
    //!
    //! Return is always int64_t
    void getCount(RegisterRef<int64_t>* node)
    {
        node->value( currentWindow.size() + nullRows);
        
    }

    //! getAvg - calculates and returns the average over the values currently
    //! in the tree.
    //!
    //! Returns 0 if the window is empty.
    void getAvg(RegisterRef<STDTYPE>* node)
    {
        node->value((0 != currentWindow.size()) ? (currentSum / static_cast<STDTYPE>(currentWindow.size())) : 0);
    }
    
private:
    winAggData currentWindow;   // Holds the values currently in the window.
    int64_t nullRows;           // Couunt of null entries
    STDTYPE currentSum;         // holds the running sum over the window.  Updated
                                // as entries are added/removed
};

FENNEL_END_NAMESPACE

#endif

// End WinAggAccum.h
