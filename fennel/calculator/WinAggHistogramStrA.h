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

#ifndef Fennel_WinAggHistogramStrA_Included
#define Fennel_WinAggHistogramStrA_Included

#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/calculator/CalcCommon.h"
#include "fennel/calculator/SqlString.h"
#include "fennel/common/TraceSource.h"

#include <utility>
#include <set>

FENNEL_BEGIN_NAMESPACE

/**
 * Support structure for calculating various windowed aggregation
 * functions (COUNT, SUM, AVG, MIN, MAX, FIRST_VALUE, LAST_VALUE)
 * against character data.
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

struct StringDesc : public TupleDatum
{
    TupleStorageByteLength cbStorage;
    StandardTypeDescriptorOrdinal mType;

    StringDesc() {}
    ~StringDesc() {}

    StringDesc(StringDesc const &other)
    {
        copyFrom(other);
    }

    StringDesc& operator = (StringDesc const &other)
    {
        copyFrom(other);
        return *this;
    }


    void copyFrom(StringDesc const &other)
    {
        TupleDatum::copyFrom(other);
        cbStorage = other.cbStorage;
        mType = other.mType;
    }

    char* pointer() const;
    TupleStorageByteLength stringLength() const;

};

class FENNEL_CALCULATOR_EXPORT WinAggHistogramStrA
{
public:
    WinAggHistogramStrA()
        : currentWindow(),
          nullRows(0),
          queue()
    {}

    ~WinAggHistogramStrA()
    {}

    typedef struct _StringDescCompare
    {
        bool operator () (const StringDesc& str1, const StringDesc& str2) const
        {
            if (!str1.isNull() && !str2.isNull()) {
                int32_t result = SqlStrCmp<1, 1>(
                    str1.pointer(), str1.stringLength(),
                    str2.pointer(), str2.stringLength());
                return result < 0;
            } else {
                return str2.isNull();
            }
        }
    } StringDescCompare;

    typedef multiset<StringDesc, StringDescCompare> WinAggData;

    typedef deque<StringDesc> WinAggQueue;

    //! addRow - Adds new value to tree and updates
    //! the running sum for current values.
    //!
    //! Input - New value to be added to the tree
    //
    void addRow(RegisterRef<char*>* node);

    //! dropRow - Removes a value from the tree and updates
    //! the running sum.
    //!
    //! Input - Value to be removed from the tree
    //
    void dropRow(RegisterRef<char*>* node);

    //! getMin - Returns the current MIN() value for the window.
    //!
    //! Returns NULL if the window is empty.
    void getMin(RegisterRef<char*>* node);

    //! getMax - Returns the current MAX() value for the window.
    //!
    //! Returns NULL if the window is empty.
    void getMax(RegisterRef<char*>* node);

    //! getFirstValue - returns the first value which entered the tree
    //!
    //! Returns NULL if the window is empty.
    void getFirstValue(RegisterRef<char*>* node);

    //! getLastValue - returns the last value which entered the tree
    //!
    //! Returns NULL if the window is empty.
    void getLastValue(RegisterRef<char*>* node);

protected:
    void setReturnReg(RegisterRef<char*>* dest, const StringDesc& src);

private:
    WinAggData currentWindow;   // Holds the values currently in the window.
    int64_t nullRows;           // Couunt of null entries

    /// FIFO queue of values, to enable FIRST_VALUE/LAST_VALUE support.
    WinAggQueue queue;
};

FENNEL_END_NAMESPACE

#endif

// End WinAggHistogramStrA.h
