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
#include "fennel/lucidera/flatfile/FlatFileBuffer.h"

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
    enum DelimiterType {
        NO_DELIM = 0,
        FIELD_DELIM,
        ROW_DELIM,
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
};

/**
 * Result of scanning for a row in a flat file format buffer
 */
class FlatFileRowParseResult
{
public:
    enum RowStatus {
        NO_STATUS = 0,
        INCOMPLETE_COLUMN,
        COLUMN_TOO_LARGE,
        NO_COLUMN_DELIM,
        TOO_FEW_COLUMNS,
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
    bool isChar;
    uint maxLength;

    FlatFileColumnDescriptor(bool isChar, uint maxLength)
    {
        this->isChar = isChar;
        this->maxLength = maxLength;
    }
};

/**
 * Describes a vector of columns to be parsed
 */
typedef std::vector<FlatFileColumnDescriptor> FlatFileRowDescriptor;

/**
 * This class parses tuples
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

public:
    /**
     * Constructs a FlatFileParser. See FlatFileExecStreamParams for more
     * detail on the parameters.
     *
     * FIXME: We can assume that the textual representation of a row can fit
     * within a single page, so we don't need to scanRow and scanRowEnd
     * separately.
     *
     * @param fieldDelim character delimiter
     *
     * @param rowDelim row delimiter
     *
     * @param quote quote character
     *
     * @param escape escape character
     */
    FlatFileParser(
        const char fieldDelim,
        const char rowDelim,
        const char quote,
        const char escape);
    
    /**
     * Scans through buffer until the end of a row is reached, and locates
     * columns within the row. May refill buffer, while preserving the row.
     *
     * @param buffer flat file buffer to be parsed
     *
     * @param current location of row inside buffer
     *
     * @param columns description of columns to be parsed
     *
     * @param result result of parsing row
     *
     * @return pointer to row
     */
    char *scanRow(
        FlatFileBuffer &buffer,
        char *current,
        FlatFileRowDescriptor columns,
        FlatFileRowParseResult &result);

    /**
     * Scans through buffer to recover from any row errors. Scans to row
     * delimiter if one was not found. Then scans past spurious row
     * delimiters. This function may refill the buffer, without preserving
     * the row, so the row should be processed before calling this function.
     *
     * @param buffer flat file buffer to be parsed
     *
     * @param current end of row scanned
     *
     * @param result result from scanning for row
     *
     * @return pointer to the next row
     */
    char *scanRowEnd(
        FlatFileBuffer &buffer,
        FlatFileRowParseResult &result);
    
    /**
     * Scan through buffer to find a row delimiter, or non row delimiter
     * character. May fill buffer without conserving the row.
     *
     * @param buffer flat file buffer to be parsed
     *
     * @param current current position in buffer
     *
     * @param search if true, look for row delimiter, else a non row delimiter
     *
     * @return pointer to character found
     */
    char *scanRowDelim(
        FlatFileBuffer &buffer,
        char *current,
        bool search);

    /**
     * Determines whether or not character is a row delimiter. If the row
     * delimiter is any of the line characters (/r or /n), then it must be
     * encoded as newline (/n) and it matches any other line character.
     */
    bool isRowDelim(char c);
    
    /**
     * Scans through buffer to find the length of a column value. Stops
     * scanning the buffer if maxLength characters have been read. 
     * Only character columns can be quoted. The column is considered to be
     * quoted if and only if the first character is a quote character.
     *
     * @param buffer buffer containing text to scan
     *
     * @param size size of buffer contents, in characters
     *
     * @param maxLength maximum length of column, excluding escapes and quotes
     *
     * @param isChar whether the column is a character column and can be quoted
     *
     * @param result result of scanning buffer
     */
    void scanColumn(
        char *buffer,
        uint size,
        uint maxLength, 
        bool isChar,
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
     * @param buffer buffer containing column value text
     *
     * @param size size of column value text, in characters
     *
     * @param untrimmed if value is untrimmed, it will be trimmed first
     *
     * @return size of resulting column text
     */
    uint stripQuoting(char *buffer, uint size, bool untrimmed);

    /**
     * Trim spaces from beginning and end of text
     *
     * @param buffer buffer containing text
     *
     * @param size size of text, in characters
     *
     * @return size of resulting column text
     */
    uint trim(char *buffer, uint size);
};

FENNEL_END_NAMESPACE

#endif

// End FlatFileParser.h
