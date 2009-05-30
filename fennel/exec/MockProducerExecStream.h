/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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

#ifndef Fennel_MockProducerExecStream_Included
#define Fennel_MockProducerExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"
#include "fennel/tuple/TupleData.h"
#include <string>
#include <iostream>

FENNEL_BEGIN_NAMESPACE

/**
 * Column generator.
 *
 * Interface for any class which generates an infinite sequence of values.
 *
 * @author Julian Hyde
 */
template <class T>
class ColumnGenerator
{
public:
    virtual ~ColumnGenerator()
    {
    }

    virtual T next() = 0;
};

typedef ColumnGenerator<int64_t> Int64ColumnGenerator;

typedef boost::shared_ptr<Int64ColumnGenerator> SharedInt64ColumnGenerator;

/**
 * MockProducerExecStreamGenerator defines an interface for generating
 * a data stream.
 */
class FENNEL_EXEC_EXPORT MockProducerExecStreamGenerator
{
public:
    virtual ~MockProducerExecStreamGenerator();

    /**
     * Generates one data value.
     *
     * @param iRow 0-based row number to generate
     * @param iCol 0-based col number to generate
     */
    virtual int64_t generateValue(uint iRow, uint iCol) = 0;
};

typedef boost::shared_ptr<MockProducerExecStreamGenerator>
    SharedMockProducerExecStreamGenerator;

typedef boost::shared_ptr<MockProducerExecStreamGenerator>
    SharedMockProducerExecStreamGenerator;

/**
 * MockProducerExecStreamParams defines parameters for MockProducerExecStream.
 */
struct FENNEL_EXEC_EXPORT MockProducerExecStreamParams
    : public SingleOutputExecStreamParams
{
    /**
     * Number of rows to generate.
     */
    uint64_t nRows;

    /**
     * Generator for row values.  If non-singular, the tuple descriptor
     * for this stream must be a single int64_t.  If singular, all output
     * is constant 0.
     */
    SharedMockProducerExecStreamGenerator pGenerator;

    /**
     * When true, save a copy of each generated tuple for later perusal.
     * Allowed only when a generator is provided.
     */
    bool saveTuples;

    /**
     * When not null, print each generated tuple to this stream, for
     * tracing or for later comparison.  Allowed only when a generator
     * is provided.
     */
    std::ostream* echoTuples;

    /**
     * Generator which determines batch size. If the generator returns a
     * non-zero value, a new batch is started. If the generator is NULL, the
     * effect is the same as a generator which always returns zero: batches are
     * created as large as possible.
     */
    SharedInt64ColumnGenerator pBatchGenerator;

    MockProducerExecStreamParams()
        : nRows(0), saveTuples(false), echoTuples(0) {}
};

/**
 * MockProducerExecStream generates mock data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT MockProducerExecStream
    : public SingleOutputExecStream
{
    uint cbTuple;
    uint64_t nRowsMax;
    uint64_t nRowsProduced;
    TupleData outputData;
    SharedMockProducerExecStreamGenerator pGenerator;
    SharedInt64ColumnGenerator pBatchGenerator;
    bool saveTuples;
    std::ostream* echoTuples;
    std::vector<std::string> savedTuples;

public:
    MockProducerExecStream();

    // implement ExecStream
    virtual void prepare(MockProducerExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);

    const std::vector<std::string>& getRowVector() {
        return const_cast<const std::vector<std::string>&>(savedTuples);
    }

    /// Returns the number of rows emitted (which does not include rows still
    /// in the output buffer).
    uint64_t getProducedRowCount();
};

FENNEL_END_NAMESPACE

#endif

// End MockProducerExecStream.h
