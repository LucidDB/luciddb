/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#ifndef Fennel_WinAggHistogram_Included
#define Fennel_WinAggHistogram_Included

#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/calculator/CalcCommon.h"
#include "fennel/common/TraceSource.h"

#include <utility>
#include <set>
#include <deque>
#include <list>

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

template <typename STDTYPE>
class WinAggHistogram
{

public:
    WinAggHistogram()
        : currentWindow(),
          nullRows(0),
          currentSum(0),
          queue()
    {}

    ~WinAggHistogram()
    {}

    typedef multiset<STDTYPE> WinAggData;

    // REVIEW: jhyde, 2006/6/14: We don't need a double-ended queue. We only
    // add to the tail, and remove from the head.
    typedef list<STDTYPE> WinAggQueue;

    //! addRow - Adds new value to tree and updates
    //! the running sum for current values.
    //!
    //! Input - New value to be added to the tree
    //
    void addRow(RegisterRef<STDTYPE>* node)
    {
        // Add the new node to the histogram strucuture

        if (!node->isNull()) {
            STDTYPE val = node->value();
            (void) currentWindow.insert(val);
            currentSum += val;

            // Add to the FIFO queue.
            queue.push_back(val);

        } else {
            ++nullRows;
        }
    }

    //! dropRow - Removes a value from the tree and updates
    //! the running sum.
    //!
    //! Input - Value to be removed from the tree
    //
    void dropRow(RegisterRef<STDTYPE>* node)
    {
        if (!node->isNull()) {
            assert(0 != currentWindow.size());
            STDTYPE* pData = node->refer();

            pair<
                typename WinAggData::iterator,
                typename WinAggData::iterator> entries =
                currentWindow.equal_range(*pData);

            assert(entries.first != entries.second);
            if (entries.first != entries.second) {
                currentWindow.erase(entries.first);
                currentSum -= *pData;
            }

            // Remove from the FIFO queue.
            queue.pop_front();
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
        node->value(currentWindow.size() + nullRows);
    }

    //! getAvg - calculates and returns the average over the values currently
    //! in the tree.
    //!
    //! Returns 0 if the window is empty.
    void getAvg(RegisterRef<STDTYPE>* node)
    {
        node->value(
            (0 != currentWindow.size())
            ? (currentSum / static_cast<STDTYPE>(currentWindow.size()))
            : 0);
    }

    //! getFirstValue - returns the first value which entered the tree
    //!
    //! Returns NULL if the window is empty.
    void getFirstValue(RegisterRef<STDTYPE>* node)
    {
        if (queue.empty()) {
            node->toNull();
        } else {
            node->value(queue.front());
        }
    }

    //! getLastValue - returns the last value which entered the tree
    //!
    //! Returns NULL if the window is empty.
    void getLastValue(RegisterRef<STDTYPE>* node)
    {
        if (queue.empty()) {
            node->toNull();
        } else {
            node->value(queue.back());
        }
    }

private:

    WinAggData currentWindow;   // Holds the values currently in the window.
    int64_t nullRows;           // Couunt of null entries

    // REVIEW (jhyde, 2006/6/14): We need to support char datatypes, so it's
    // not appropriate that sum has the same type as the values in the
    // window. Maybe break histogram into a base class only min/max support,
    // and a derived class with sum. The sum type will be an extra template
    // parameter.

    /// Holds the running sum over the window.  Updated
    /// as entries are added/removed
    STDTYPE currentSum;

    /// FIFO queue of values, to enable FIRST_VALUE/LAST_VALUE support.
    WinAggQueue queue;
};

FENNEL_END_NAMESPACE

#endif

// End WinAggHistogram.h
