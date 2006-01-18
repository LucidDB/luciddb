/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2005-2005 John V. Sichi
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

#ifndef Fennel_MockConsumerExecStream_Included
#define Fennel_MockConsumerExecStream_Included

#include "fennel/exec/SingleInputExecStream.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TuplePrinter.h"

using std::vector;
using std::string;
using std::ostream;

FENNEL_BEGIN_NAMESPACE

/**
 * MockConsumerExecStreamParams defines parameters for MockConsumerExecStream.
 */
struct MockConsumerExecStreamParams : public SingleInputExecStreamParams
{
    /** save data as a vector of strings */
    bool saveData;
    /** when not null, echo data to this stream */
    ostream* echoData;

    MockConsumerExecStreamParams() :saveData(true), echoData(0) {}
};

/**
 * MockConsumerExecStream consumes data from a single input. It saves the data
 * as a vector of strings, or echoes the strings to an ostream, or both.
 *
 * @author Julian Hyde
 * @version $Id$
 */
class MockConsumerExecStream : public SingleInputExecStream
{
    bool saveData;
    ostream* echoData;
    long rowCount;
    TupleData inputTuple;
    TuplePrinter tuplePrinter;
    vector<string> rowStrings;
    bool recvEOS;
public:
    // implement ExecStream
    virtual void prepare(MockConsumerExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);

    long getRowCount() const { return rowCount; }
    vector<string>& getRowVector() { return rowStrings; }
    bool getRecvEOS() const { return recvEOS; }
};

FENNEL_END_NAMESPACE

#endif

// End MockConsumerExecStream.h
