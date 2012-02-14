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

#ifndef Fennel_FlatFileExecStream_Included
#define Fennel_FlatFileExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Describes the type of scan to be performed by an ExecStream
 */
enum FlatFileMode
{
    FLATFILE_MODE_QUERY = 0,
    FLATFILE_MODE_DESCRIBE,
    FLATFILE_MODE_SAMPLE,
    FLATFILE_MODE_QUERY_TEXT
};

/**
 * FlatFileExecStreamParams defines parameters for instantiating a
 * FlatFileExecStream. Currently, ASCII data is supported. More
 * parameters may be needed to support internationalization.
 *
 * <p>
 *
 * TODO: review whether it is ok to infer parsing and storage behavior
 * from output tuple type. Probably should parse field and row delim
 * on the Java side.
 */
struct FlatFileExecStreamParams : public SingleOutputExecStreamParams
{
    // TODO: Codepage support

    /**
     * Path to the flat file containing tuples to be read. This path follows
     * conventions of the operating system.
     */
    std::string dataFilePath;

    /**
     * Path to the error log used for writing errors encountered while
     * processing tuples. If this value is empty, then no logging will
     * be performed.
     */
    std::string errorFilePath;

    /**
     * Delimiter used to separate fields in a row. This value is typically
     * ',' (comma) or '\\t' (tab) or zero, which signifies no delimiter.
     */
    char fieldDelim;

    /**
     * Delimiter used to terminate a row. This value is typically
     * '\\n' (newline), which represents any combination of '\\r' and '\\n'.
     */
    char rowDelim;

    /**
     * Character used to quote data values. Quoted data values must have an
     * opening and terminating quote character. Special characters, such as
     * delimiter characters, may be quoted. The quote character may be empty,
     * or may be a single character.
     */
    char quoteChar;

    /**
     * Ignored outside of quoted values. This character quotes the quote
     * character itself, or other any character. Within the context of a quoted
     * data value, if the escape character is the same as the quote character,
     * then a single quote continues to represent a closing quote, but a
     * contiguous pair is represents a quote embedded into the data value.
     */
    char escapeChar;

    /**
     * Specifies whether the flat file contains a header. If a header is
     * specified, it is expected to take up the first line of the file, so
     * this line is skipped. Defaults to false.
     */
    bool header;

    /**
     * Specifies number of rows to scan when sampling data.
     */
    int numRowsScan;

    /**
     * Converts flat file text into typed data
     */
    std::string calcProgram;

    /**
     * Mode in which to run the flatfile scan
     */
    FlatFileMode mode;

    /**
     * The maximum number of errors to allow before failing. Resets when the
     * stream is reopened. A value of -1 indicates that there is no max.
     */
    int errorMax;

    /**
     * The maximum number of errors to log. Resets when the stream is
     * reopened. A value of -1 indicates that there is no max.
     */
    int errorLogMax;

    /**
     * Whether to be lenient when reading flatfile columns. If columns are
     * missing at the end of a row, they are treated as null. Unexpected
     * columns at the end of a row are ignored.
     */
    bool lenient;

    /**
     * Whether to trim output columns
     */
    bool trim;

    /**
     * Whether to map source columns to target columns by name. Requires a
     * header, to specify source column names, and target column names.
     * Missing columns are filled in with null.
     */
    bool mapped;

    /**
     * Names of the target columns.
     */
    std::vector<std::string> columnNames;

    explicit FlatFileExecStreamParams()
    {
        errorFilePath = "";
        fieldDelim = ',';
        rowDelim = '\n';
        quoteChar = '"';
        escapeChar = '\\';
        header = true;
        numRowsScan = 0;
        mode = FLATFILE_MODE_QUERY;
    }
};

/**
 * FlatFileExecStream parses a text file and produces tuples according to a
 * specified column format. Options support a variety of delimiters and
 * quoting styles. Processing errors are optionally logged to a related file.
 *
 * <p>
 *
 * The actual implementation is in FlatFileExecStreamImpl.
 *
 * @author jpham
 * @version $Id$
 */
class FENNEL_FLATFILE_EXPORT FlatFileExecStream
    : public SingleOutputExecStream
{
public:
    /**
     * Factory method.
     *
     * @return new FlatFileExecStream instance
     */
    static FlatFileExecStream *newFlatFileExecStream();

    // implement ExecStream
    virtual void prepare(FlatFileExecStreamParams const &params) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End FlatFileExecStream.h
