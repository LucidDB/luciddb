/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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
    virtual ~ColumnGenerator() {}
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
    MockProducerExecStreamGenerator();
    virtual ~MockProducerExecStreamGenerator();

    /**
     * Called before generating each row. Decides when to end the current batch
     * and start a new one. By default, evaluates the functor
     * endsBatchPredicate, but a subclass can override that.
     *
     * @return false to add another row to current batch, true to end the batch.
     * @param nrows number of rows written so far; thus, 0 before the first row,
     * 1 after it, etc.
     */
    virtual bool endsBatch(uint nrows);

    /**
     * Generates one data value.
     *
     * @param iRow 0-based row number to generate
     * @param iCol 0-based col number to generate
     */
    virtual int64_t generateValue(uint iRow, uint iCol) = 0;

    /** a functor that maps row number to bool:
     * base class returns a fixed default.
     */
    class RowPredicate {
        bool defaultValue;
    public:
        RowPredicate(bool = false);
        virtual ~RowPredicate();
        virtual bool operator()(uint iRow);
    };
    typedef boost::shared_ptr<RowPredicate> SharedRowPredicate;

    /** Sets the functor used by endsBatch */
    void setEndsBatchPredicate(SharedRowPredicate);

private:
    SharedRowPredicate endsBatchPredicate; // null means always return false
};

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
     * Generator for row values. All output columns are int64_t.
     * If this is null, all output columsns are 0.
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
    bool saveTuples;
    std::ostream* echoTuples;
    std::vector<std::string> savedTuples;

protected:
    virtual ExecStreamResult innerExecute(ExecStreamQuantum const&);

    /**
     * called after writing last row of a batch
     * @param nrows counts all rows written
     */
    virtual ExecStreamResult onEndOfBatch(uint nrows);

    SharedMockProducerExecStreamGenerator getGenerator();
    // returns the number of rows generated and written to output buffer
    // cf getProducedRowCoutnt()
    uint64_t getGeneratedRowCount();

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
