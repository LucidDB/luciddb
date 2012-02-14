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

#ifndef Fennel_FlatFileExecStreamImpl_Included
#define Fennel_FlatFileExecStreamImpl_Included

#include "fennel/flatfile/FlatFileBuffer.h"
#include "fennel/flatfile/FlatFileExecStream.h"
#include "fennel/flatfile/FlatFileParser.h"
#include "fennel/segment/SegmentAccessor.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/tuple/TupleData.h"

#include <boost/scoped_ptr.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * The maximum length of a column name inside of a header
 */
const int FLAT_FILE_MAX_COLUMN_NAME_LEN = 255;

/**
 * The maximum length of text to scan for a non character column
 */
const int FLAT_FILE_MAX_NON_CHAR_VALUE_LEN = 255;

/**
 * FlatFileExecStreamImpl implements the FlatFileExecStream interface.
 *
 * @author John Pham
 * @version $Id$
 */
class FENNEL_FLATFILE_EXPORT FlatFileExecStreamImpl
    : public FlatFileExecStream
{
    // max length of text for a row when signalling an error
    static const uint MAX_ROW_ERROR_TEXT_WIDTH;

    // parameters
    std::string dataFilePath;
    bool header;
    bool lenient;
    bool trim;
    bool mapped;
    std::vector<std::string> columnNames;

    FlatFileRowDescriptor rowDesc;
    SharedFlatFileBuffer pBuffer;
    PBuffer pBufferStorage;
    char *next;
    SharedFlatFileParser pParser;
    FlatFileRowParseResult lastResult;
    TupleDescriptor textDesc;
    TupleData textTuple, dataTuple;
    bool isRowPending;

    // for sampling/describe mode
    FlatFileMode mode;
    int numRowsScan;
    bool done;
    VectorOfUint fieldSizes;
    std::string describeResult;

    SegPageLock bufferLock;
    SegmentAccessor scratchAccessor;

    // error handling
    uint nRowsOutput, nRowErrors;
    std::string reason;
    TupleDescriptor errorDesc;
    TupleData errorTuple;

    // implement ExecStream
    virtual void closeImpl();

    /**
     * Releases resources associated with this stream.
     */
    void releaseResources();

    /**
     * Finds an output column by its name and returns the column's index.
     * Performs a case insensitive comparison and uses the first matching
     * column. If column could not be found, this function returns MAXU.
     */
    uint findField(const std::string &name);

    /**
     * Translates a TupleDescriptor into a FlatFileRowDescriptor. The major
     * attributes required for parsing a column are whether it is a character
     * column (which can be quoted) and the maximum length of the column.
     *
     * <p>
     *
     * If the column is a character column, it's maximum length is determined
     * by the maximum size of the column. A default length is used for other
     * columns.
     *
     * @param tupleDesc tuple descriptor used for inferring row descriptor
     */
    FlatFileRowDescriptor readTupleDescriptor(
        const TupleDescriptor &tupleDesc);

    /**
     * Processes a row of input data. For regular queries and sampling
     * queries, this produces a tuple. However, for a describe, a tuple
     * is not produced until the end.
     *
     * @param result result of parsing text row
     *
     * @param tuple tuple data
     */
    void handleTuple(
        FlatFileRowParseResult &result,
        TupleData &tuple);

    /**
     * Based on rows sampled, generate an output row with a description
     * of the stream.
     */
    void describeStream(TupleData &tupleData);

    /**
     * Logs a single error to file. The reason for the error is read
     * from the row parse result.
     */
    void logError(const FlatFileRowParseResult &result);

    /**
     * Logs a single error to file
     */
    void logError(
        const std::string reason,
        const FlatFileRowParseResult &result);

    /**
     * Throws an error if a row delimiter was not found.
     */
    void checkRowDelimiter();

public:
    // implement ExecStream
    virtual void prepare(FlatFileExecStreamParams const &params);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End FlatFileExecStreamImpl.h
