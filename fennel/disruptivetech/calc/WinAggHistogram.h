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

#ifndef Fennel_WinAggAccum_Included
#define Fennel_WinAggAccum_Included

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
        currentSum(0),
        currentWindow(),
        nullRows()
    {}

    ~WinAggHistogram()
    {}

    // Comparator object supplied to the window object for comparing entires
    // during sort.
    typedef struct _TupleDataCompare
    {
        bool operator () (const TupleDatum* d1, const TupleDatum* d2) const
        {
            if (d1 && d2) {
                return *(reinterpret_cast<STDTYPE*>(const_cast<PBuffer>(d1->pData))) <
                    *(reinterpret_cast<STDTYPE*>(const_cast<PBuffer>(d2->pData)));
            } else {
                return (NULL == d2);
            }
        }
    } TupleDataCompare;
        
    typedef multiset<TupleDatum*, TupleDataCompare> winAggData;

    // data types to do NULL row accounting
    typedef struct _TuplePtrCompare
    {
        bool operator () (const TupleDatum* v1, const TupleDatum* v2) const
        {
            return( v1 < v2);
        }
    } TuplePtrCompare;
    
    typedef set<TupleDatum*, TuplePtrCompare> winAggNullData;
    

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
            (void) currentWindow.insert(pDatum);

            // Update the running sum for the accumulator
            STDTYPE val = node->value();
            currentSum += val;
        } else {
            (void)nullRows.insert(pDatum);
        }
    }
    
    
    //! addRow - Adds new value to tree and updates
    //! the running sum for current values.
    //!
    //! Input - New value to be added to the tree
    //
    void dropRow(RegisterRef<STDTYPE>* node)
    {
        TupleDatum *pDatum = node->getBinding();

        if (!pDatum->isNull()) {
            assert(0 != currentWindow.size());
        
            // remove the entry from the accumulator.  The entries are in a
            // multiset. The key is the the addresess of the data set. A comparator
            // was supplied to sort by the tuple contents.
            STDTYPE* pData = node->refer();

            pair<typename winAggData::iterator, typename winAggData::iterator> entries =
                currentWindow.equal_range(pDatum);
            for(; entries.first != entries.second; entries.first++) {
                if (pDatum == *(entries.first)) {
                    currentWindow.erase(entries.first);
                    
                    // reduce the running sum for the window
                    currentSum -= *pData;
                    break;
                }
            }
        } else {
            assert(0 != nullRows.size());
            int count = nullRows.erase( pDatum);
            assert(1 == count);
        }
    }

    //! getMin - Returns the current MIN() value for the window.
    //!
    //! Returns 0 if the window is empty.
    void getMin(RegisterRef<STDTYPE>* node)
    {
        if (0 != currentWindow.size()) {
            // TupleDatum *pDatum = *(currentWindow.begin());
            TupleDatum *td = *(currentWindow.begin());
            PBuffer pBuffer = const_cast<PBuffer>(td->pData);
            STDTYPE *pData = reinterpret_cast<STDTYPE*>(pBuffer);
            
            
//            STDTYPE *pData = reinterpret_cast<STDTYPE*>
//                (const_cast<PBuffer>((*(currentWindow.begin()))->pData));
            node->value(*pData);
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
            node->value(*reinterpret_cast<STDTYPE*>
                (const_cast<PBuffer>((*(--(currentWindow.end())))->pData)));
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
        node->value( currentWindow.size() + nullRows.size());
        
    }

    //! getAvg - calculates and returns the average over the values currently
    //! in the tree.
    //!
    //! Returns 0 if the window is empty.
    void getAvg(RegisterRef<STDTYPE>* node)
    {
        node->value((0 != currentWindow.size()) ? (currentSum / static_cast<STDTYPE>(currentWindow.size())) : 0);
    }
    

    // Private Data
private:
    
    STDTYPE currentSum;     // holds the running sum over the window.  Updated
                            // as entries are added/removed
    winAggData currentWindow; // Holds the values currently in the window.
    winAggNullData nullRows;  // Holds Ptrs to Tuples with NULL data entries
};



FENNEL_END_NAMESPACE

#endif

// End WinAggAccum.h
