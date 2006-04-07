/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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

#ifndef Fennel_FlatFileParser_Included
#define Fennel_FlatFileParser_Included

#include "fennel/common/CommonPreamble.h"

#include <vector>

FENNEL_BEGIN_NAMESPACE

class FlatFileParser;
typedef boost::shared_ptr<FlatFileParser> SharedFlatFileParser;

/**
 * Result of scanning for a column in a flat file format buffer
 */
class FlatFileColumnParseResult
{
public:
    /** Delimiter type encountered during column parse */
    enum DelimiterType {
        /** No delimiter was found, or delimiter was uncertain */
        NO_DELIM = 0,
        /** Field delimiter was found */
        FIELD_DELIM,
        /** Row delimiter was found */
        ROW_DELIM,
        /** Parser read up to maximum length of field */
        MAX_LENGTH
    };
    
    /**
     * Delimiter type encountered during parsing
     */
    DelimiterType type;

    /**
     * Size of column parsed
     */
    uint size;

    /**
     * Reference to the next column to be parsed
     */
    char *next;

    /**
     * Sets the fields of a column parse result, based on the result
     * type and the size of the column
     */
    void setResult(DelimiterType type, char *buffer, uint size);
};

/**
 * Result of scanning for a row in a flat file format buffer
 */
class FlatFileRowParseResult
{
public:
    /** Status of row parsing */
    enum RowStatus {
        /**
         * Row was parsed successfully
         */
        NO_STATUS = 0,
        /**
         * A column in the row was not delimited by either a column or
         * row delimiter
         */
        INCOMPLETE_COLUMN,
        /**
         * Row did not fit into buffer
         */
        ROW_TOO_LARGE,
        /**
         * Row delimiter was hit, before any column delimiters, when
         * multiple columns were expected
         */
        NO_COLUMN_DELIM,
        /**
         * Column delimiters were encountered, but row had too few columns
         */
        TOO_FEW_COLUMNS,
        /**
         * Row had too many columns, or column values were too long,  
         */
        TOO_MANY_COLUMNS
    };

    explicit FlatFileRowParseResult();
    void reset();
    
    /**
     * Reports errors encountered during row parsing
     */
    RowStatus status;

    /**
     * Offsets to column values within the buffer
     */
    std::vector<uint> offsets;

    /**
     * Sizes of column values
     */
    std::vector<uint> sizes;
    
    /**
     * Reference to the current row.
     */
    char *current;

    /**
     * Reference to the next row to be parsed. After a row parse, this will be
     * set to the first unread character, regardless of errors.
     */
    char *next;

    /**
     * Ongoing count of row delimiters read
     */
    uint nRowDelimsRead;
};

/**
 * Describes characteristics of a column to be parsed
 */
class FlatFileColumnDescriptor
{
public:
    uint maxLength;

    FlatFileColumnDescriptor(uint maxLength)
    {
        this->maxLength = maxLength;
    }
};

/**
 * Describes a vector of columns to be parsed.
 *
 * <p>
 *
 * There are two main types of scans. An unbounded scan and a bounded
 * scan. By default, the scan is bounded.
 */
class FlatFileRowDescriptor : public std::vector<FlatFileColumnDescriptor> 
{
    bool bounded;

public:
    /**
     * Maximum number of columns in an unbounded scan
     */
    static const int MAX_COLUMNS = 1024;

    /**
     * Maximum length of a column in an unbounded scan. This is based on
     * the default farrago maximum length.
     */
    static const int MAX_COLUMN_LENGTH = 65535;
    
    /**
     * Construct a new row descriptor
     */
    FlatFileRowDescriptor();

    /**
     * Set scan to run in an unbounded mode, with a dynamic number of
     * columns, and unbounded column sizes
     */
    void setUnbounded();
    
    /**
     * Whether to run regular scan mode, bounded by column descriptions,
     * or to run in an unbounded scan mode
     */
    bool isBounded() const;
};

/**
 * This class parses fields and rows from a field delimited text buffer.
 * The main entry point is <code>scanRow()</code> which returns pointers
 * into a text buffer, representing columns. The method
 * <code>stripQuoting()</code> may be useful for decoding a quoted value.
 * Other methods are primarily made public for testing purposes.
 *
 * @author John Pham
 * @version $Id$
 */
class FlatFileParser
{
    char fieldDelim;
    char rowDelim;
    char quote;
    char escape;

    /**
     * Whether to perform fixed mode parsing
     */
    bool fixed;

    /**
     * Scans through buffer to recover from any row errors. Scans to row
     * delimiter if one was not found. Then scans past spurious row
     * delimiters. On success, increments row delimiter count.
     *
     * @param[in] buffer flat file buffer to be parsed
     *
     * @param[in] size size of buffer
     *
     * @param[in, out] result result from scanning for row
     *
     * @return pointer to the next row, or end of buffer
     */
    const char *scanRowEnd(
        const char *buffer,
        int size,
        FlatFileRowParseResult &result);
    
    /**
     * Scan through buffer to find a row delimiter, or non row delimiter
     * character.
     *
     * @param[in] buffer flat file buffer to be parsed
     *
     * @param[in] size size of buffer
     *
     * @param[in] search if true, look for row delimiter, else a non row delim
     *
     * @return pointer to character found, or end of buffer
     */
    const char *scanRowDelim(
        const char *buffer,
        int size,
        bool search);

    /**
     * Determines whether or not character is a row delimiter. If the row
     * delimiter is any of the line characters (/r or /n), then it must be
     * encoded as newline (/n) and it matches any other line character.
     */
    bool isRowDelim(char c);

public:
    /**
     * Constructs a FlatFileParser. See FlatFileExecStreamParams for more
     * detail on the parameters.
     *
     * @param[in] fieldDelim character delimiter
     *
     * @param[in] rowDelim row delimiter
     *
     * @param[in] quote quote character
     *
     * @param[in] escape escape character
     */
    FlatFileParser(
        const char fieldDelim,
        const char rowDelim,
        const char quote,
        const char escape);
    
    /**
     * Scans through buffer until the end of a row is reached, and locates
     * columns within the row.
     *
     * @param[in] buffer buffer with text to be parsed
     *
     * @param[in] size size of buffer, in characters
     *
     * @param[in] columns description of columns to be parsed
     *
     * @param[out] result result of parsing row
     */
    void scanRow(
        const char *buffer, 
        int size,
        const FlatFileRowDescriptor &columns,
        FlatFileRowParseResult &result);

    /**
     * Scans through buffer to find the length of a column value. Keeps
     * going until it reads a delimiter or it completes a fixed column.
     * A column is considered to be quoted if and only if the first
     * character is a quote character.
     *
     * @param[in] buffer buffer containing text to scan
     *
     * @param[in] size size of buffer contents, in characters
     *
     * @param[in] maxLength max length of column, excluding escapes and quotes
     *
     * @param[out] result result of scanning buffer
     */
    void scanColumn(
        const char *buffer,
        uint size,
        uint maxLength, 
        FlatFileColumnParseResult &result);


    /**
     * Scans a fixed format column. In this mode, the quote character and
     * column delimiter are ignored. It is possible to stop parsing because
     * (1) a row delimiter is read (2) the max length is reached or (3) the
     * end of buffer is reached
     */
    void scanFixedColumn(
        const char *buffer,
        uint size,
        uint maxLength, 
        FlatFileColumnParseResult &result);
    
    /**
     * Removes quoting and escape characters from a column value. If untrimmed
     * is set, then the value will be trimmed first. Otherwise, quoted values 
     * are expected to begin and end with a quote.
     *
     * <p>
     *
     * Examples (assuming both quote and escape are double quote): <br>
     * <code>"a quote"</code> becomes <code>a quote</code> <br>
     * <code>"""a quote"""</code> becomes <code>"a quote"</code> <br>
     * <code>"a quote</code> becomes <code>a quote</code> <br>
     * <code>a quote"</code> becomes <code>a quote"</code> <br>
     * <code>""aquote"</code> becomes <code>(empty string)</code>
     *
     * @param[in, out] buffer buffer containing column value text
     *
     * @param[in] size size of column value text, in characters
     *
     * @param[in] untrimmed if value is untrimmed, it will be trimmed first
     *
     * @return size of resulting column text
     */
    uint stripQuoting(char *buffer, uint size, bool untrimmed);

    /**
     * Trim spaces from beginning and end of text
     *
     * @param[in, out] buffer buffer containing text
     *
     * @param[in] size size of text, in characters
     *
     * @return size of resulting column text
     */
    uint trim(char *buffer, uint size);
};

FENNEL_END_NAMESPACE

#endif

// End FlatFileParser.h
