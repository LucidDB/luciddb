/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2005 John V. Sichi
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
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TuplePrinter.h"

using std::vector;
using std::string;
using std::ostream;

FENNEL_BEGIN_NAMESPACE

/**
 * MockConsumerExecStreamTupleChecker is an abstract object used to check rows
 * of actual data against their expected values. It is a kind of inverse to
 * MockProducerExecStreamGenerator. It is up to a subclass to define the
 * expected values.
 */
struct FENNEL_EXEC_EXPORT MockConsumerExecStreamTupleChecker {
    virtual ~MockConsumerExecStreamTupleChecker();
    /** Checks an actual tuple against the its expected value.
     * @return true when actual matches expected, otherwise false. But usually
     *  called in a unit test for a side effect.
     * @param n row sequence number, starting at 0, increasing til EOS.
     * @param desc describes the tuple.
     * @param actual the actual tuple.
     */
    virtual bool check(
        int n, const TupleDescriptor& desc, const TupleData& actual) = 0;
};

/**
 * MockConsumerExecStreamParams defines parameters for MockConsumerExecStream.
 */
struct FENNEL_EXEC_EXPORT MockConsumerExecStreamParams
    : public SingleInputExecStreamParams
{
    /** flag: save data as a vector of strings */
    bool saveData;
    /** unless null, echo data to this stream */
    ostream* echoData;
    /** unless null, call this to check each tuple received */
    MockConsumerExecStreamTupleChecker* checkData;

    /** a kind of fetch timeout: a limit to the number of consecutive
     * EXECRC_BUF_UNDERFLOWs returned from execute(): if the limit is reached,
     * it returns EXECRC_EOS. 0 means no limit */
    int maxConsecutiveUnderflows;

    /** to test error handling. throw a FennelExcn on reading the Nth row.
     * 1 means the first row, 0 means never (the default).
     */
    int dieOnNthRow;

    MockConsumerExecStreamParams()
        : saveData(false), echoData(0), checkData(0),
        maxConsecutiveUnderflows(0), dieOnNthRow(0)
    {
    }
};

/**
 * MockConsumerExecStream consumes data from a single input.
 * It can: verify each row of data by calling a functor; save the data as a
 * vector of strings; echo the strings to an ostream.
 *
 * @author Julian Hyde
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT MockConsumerExecStream
    : public SingleInputExecStream
{
protected:
    bool saveData;
    int maxConsecutiveUnderflows;
    int dieOnNthRow;
    ostream* echoData;
    MockConsumerExecStreamTupleChecker* checkData;
    vector<string> rowStrings;
private:
    long rowCount;
    long incorrectRowCount;
    int consecutiveUnderflowCt;
    TupleData inputTuple;
    TuplePrinter tuplePrinter;
    bool recvEOS;
    ExecStreamResult innerExecute(ExecStreamQuantum const&);

public:
    MockConsumerExecStream();
    virtual ~MockConsumerExecStream();

    // implement ExecStream
    virtual void prepare(MockConsumerExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);

    long getRowCount() const {
        return rowCount;
    }

    long getIncorrectRowCount() const {
        return incorrectRowCount;
    }

    const vector<string>& getRowVector() {
        return const_cast<const vector<string>& >(rowStrings);
    }

    bool getRecvEOS() const
    {
        return recvEOS;
    }
};

FENNEL_END_NAMESPACE

#endif

// End MockConsumerExecStream.h
