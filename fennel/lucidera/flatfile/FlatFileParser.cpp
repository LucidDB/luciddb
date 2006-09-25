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

#include "fennel/common/CommonPreamble.h"
#include "fennel/lucidera/flatfile/FlatFileParser.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void FlatFileColumnParseResult::setResult(
    FlatFileColumnParseResult::DelimiterType type, char *buffer, uint size)
{
    this->type = type;
    this->size = size;

    next = buffer + size;
    switch (type) {
    case NO_DELIM:
    case MAX_LENGTH:
        break;
    case FlatFileColumnParseResult::FIELD_DELIM:
    case FlatFileColumnParseResult::ROW_DELIM:
        next++;
        break;
    default:
        permAssert(false);
    }
}

FlatFileRowDescriptor::FlatFileRowDescriptor() :
    std::vector<FlatFileColumnDescriptor>()
{
    bounded = true;
}

void FlatFileRowDescriptor::setUnbounded() 
{
    bounded = false;
}

bool FlatFileRowDescriptor::isBounded() const
{
    return bounded;
}

FlatFileRowParseResult::FlatFileRowParseResult()
{
    reset();
}

void FlatFileRowParseResult::reset()
{
    status = NO_STATUS;
    current = next = NULL;
    nRowDelimsRead = 0;
}

FlatFileParser::FlatFileParser(
    char fieldDelim, char rowDelim, char quote, char escape)
{
    this->fieldDelim = fieldDelim;
    this->rowDelim = rowDelim;
    this->quote = quote;
    this->escape = escape;

    fixed = (fieldDelim == 0);
    if (fixed) {
        assert(quote == 0);
        assert(escape == 0);
    }
}

void FlatFileParser::scanRow(
    const char *buffer, 
    int size, 
    const FlatFileRowDescriptor &columns,
    FlatFileRowParseResult &result)
{
    assert(size >= 0);
    const char *row = buffer;
    uint offset = 0;
    FlatFileColumnParseResult columnResult;

    result.status = FlatFileRowParseResult::NO_STATUS;
    bool bounded = columns.isBounded();
    bool lenient = columns.isLenient();
    bool mapped = columns.isMapped();
    bool strict = (bounded && (!lenient));

    uint maxColumns;
    if (bounded) {
        maxColumns = columns.size();
        result.resize(maxColumns);
        for (uint i = 0; i < maxColumns; i++) {
            result.setNull(i);
        }
    } else {
        maxColumns = FlatFileRowDescriptor::MAX_COLUMNS;
        result.clear();
    }

    // Scan any initial row delimiters, helps for the case when a row
    // delimiter is multiple characters like \r\n and the delimiter
    // characters are split between two buffers. (The previous row could
    // be complete due to \r, and parsing could begin at \n.
    const char *nonDelim = scanRowDelim(row, size, false);
    offset = nonDelim - row;

    bool done = false;
    uint maxLength = FlatFileRowDescriptor::MAX_COLUMN_LENGTH;
    for (uint i = 0; i < maxColumns; i++) {
        if (bounded) {
            maxLength = columns[i].maxLength;
        }
        scanColumn(
            row + offset,
            size - offset,
            maxLength,
            columnResult);
        switch (columnResult.type) {
        case FlatFileColumnParseResult::NO_DELIM:
            result.status = FlatFileRowParseResult::INCOMPLETE_COLUMN;
            done = true;
            break;
        case FlatFileColumnParseResult::ROW_DELIM:
            if (strict && (i+1 != columns.size())) {
                if (i == 0) {
                    result.status = FlatFileRowParseResult::NO_COLUMN_DELIM;
                } else {
                    result.status = FlatFileRowParseResult::TOO_FEW_COLUMNS;
                }
            }
            done = true;
            break;
        case FlatFileColumnParseResult::MAX_LENGTH:
        case FlatFileColumnParseResult::FIELD_DELIM:
            if (strict && (i+1 == columns.size())) {
                result.status = FlatFileRowParseResult::TOO_MANY_COLUMNS;
                done = true;
            }
            break;
        default:
            permAssert(false);
        }
        if (bounded) {
            int target = mapped ? columns.getMap(i) : i;
            if (target >= 0) {
                assert (target < maxColumns);
                result.setColumn(target, offset, columnResult.size);
            }
        } else {
            result.addColumn(offset, columnResult.size);
        }
        offset = columnResult.next - row;
        if (done) break;
    }
    result.current = const_cast<char *>(row);
    result.next = const_cast<char *>(
        scanRowEnd(columnResult.next, buffer+size-columnResult.next, result));
}

const char *FlatFileParser::scanRowEnd(
    const char *buffer,
    int size,
    FlatFileRowParseResult &result)
{
    const char *read = buffer;
    const char *end = buffer + size;
    switch (result.status) {
    case FlatFileRowParseResult::INCOMPLETE_COLUMN:
    case FlatFileRowParseResult::ROW_TOO_LARGE:
        assert(read == end);
        return read;
    case FlatFileRowParseResult::TOO_MANY_COLUMNS:
        read = scanRowDelim(read, end-read, true);
        if (read == end) {
            return read;
        }
    case FlatFileRowParseResult::NO_STATUS:
    case FlatFileRowParseResult::NO_COLUMN_DELIM:
    case FlatFileRowParseResult::TOO_FEW_COLUMNS:
        read = scanRowDelim(read, end-read, false);
        break;
    default:
        permAssert(false);
    }
    result.nRowDelimsRead++;
    return read;
}

const char *FlatFileParser::scanRowDelim(
    const char *buffer, 
    int size, 
    bool search) 
{
    const char *read = buffer;
    const char *end = buffer + size;
    while (read < end) {
        if (isRowDelim(*read) == search) {
            break;
        } else {
            read++;
        }
    }
    return read;
}

bool FlatFileParser::isRowDelim(char c)
{
    assert(rowDelim != '\r');
    return (rowDelim == '\n') ? (c == '\r' || c == '\n') : (c == rowDelim);
}

void FlatFileParser::scanColumn(
    const char *buffer,
    uint size,
    uint maxLength, 
    FlatFileColumnParseResult &result)
{
    if (fixed) {
        return scanFixedColumn(buffer, size, maxLength, result);
    }

    assert(buffer != NULL);
    const char *read = buffer;
    const char *end = buffer + size;
    bool quoted = (size > 0 && *buffer == quote);
    bool quoteEscape = (quoted && quote == escape);

    FlatFileColumnParseResult::DelimiterType type =
        FlatFileColumnParseResult::NO_DELIM;
    if (quoted) {
        read++;
    }
    while (read < end) {
        if (*read == quote) {
            read++;
            if (quoteEscape) {
                // read next character to determine whether purpose of
                // this character is an escape character or an end quote
                if (read == end) {
                    break;
                }
                if (*read == quote) {
                    // two consecutive quote/escape characters is an
                    // escaped quote
                    read++;
                    continue;
                }
            }
            if (quoted) {
                // otherwise a quote may be a close quote
                quoteEscape = quoted = false;
            }
        } else if (*read == escape) {
            read++;
            // an escape escapes the next character
            if (read == end) {
                break;
            }
            read++;
        } else if (quoted) {
            read++;
        } else if (*read == fieldDelim) {
            type = FlatFileColumnParseResult::FIELD_DELIM;
            break;
        } else if (isRowDelim(*read)) {
            type = FlatFileColumnParseResult::ROW_DELIM;
            break;
        } else {
            read++;
        }
    }
    
    uint resultSize = read - buffer;
    result.setResult(type, const_cast<char *>(buffer), resultSize);
}

void FlatFileParser::scanFixedColumn(
    const char *buffer,
    uint size,
    uint maxLength, 
    FlatFileColumnParseResult &result)
{
    assert(buffer != NULL);
    const char *read = buffer;
    const char *end = buffer + size;
    uint remaining = maxLength;

    FlatFileColumnParseResult::DelimiterType type =
        FlatFileColumnParseResult::NO_DELIM;
    while (read < end && remaining > 0) {
        if (isRowDelim(*read)) {
            type = FlatFileColumnParseResult::ROW_DELIM;
            break;
        }
        read++;
        remaining--;
    }

    // Resolve delimiter type if another character can be read. This allows
    // us to catch the case where a row delimiter follows a max length field.
    if (type == FlatFileColumnParseResult::NO_DELIM && read < end) {
        if (isRowDelim(*read)) {
            type = FlatFileColumnParseResult::ROW_DELIM;
        } else if (remaining == 0) {
            type = FlatFileColumnParseResult::MAX_LENGTH;
        }
    }

    uint resultSize = read - buffer;
    result.setResult(type, const_cast<char *>(buffer), resultSize);
}

void FlatFileParser::stripQuoting(
    FlatFileRowParseResult &rowResult,
    bool trim)
{
    int nFields = rowResult.getReadCount();

    if (rowResult.strippedSizes.size() < nFields) {
        rowResult.strippedSizes.resize(nFields);
    }

    for (uint i = 0; i < nFields; i++) {
        char *value = rowResult.getColumn(i);
        uint newSize = 0;
        if (value != NULL) {
            uint oldSize = rowResult.getRawColumnSize(i);
            newSize = stripQuoting(value, oldSize, trim);
        }
        rowResult.strippedSizes[i] = newSize;
    }
}

uint FlatFileParser::stripQuoting(
    char *buffer, uint sizeIn, bool untrimmed) 
{
    assert(buffer != NULL);
    if (sizeIn == 0) {
        return 0;
    }
    int size = untrimmed ? trim(buffer, sizeIn) : sizeIn;
    bool quoted = false;
    char *read = buffer;
    char *end = buffer + size;
    char *write = buffer;

    if (*buffer == quote) {
        quoted = true;
        read++;
    }
    bool quoteEscape = (quoted && quote == escape);
    while (read < end) {
        if (quoteEscape && *read == quote) {
            read++;
            if ((read < end) && (*read == quote)) {
                // two consecutive quote/escape characters is an escaped quote
                *write++ = *read++;
            } else {
                // single quote/escape is end quote
                break;
            }
        } else if (quoted && *read == quote) {
            break;
        } else if (*read == escape) {
            read++;
            if (read < end) {
                *write++ = *read++;
            }
        } else {
            *write++ = *read++;
        }
    }
    return write-buffer;
}

uint FlatFileParser::trim(char *buffer, uint size)
{
    assert(buffer != NULL);
    if (size == 0) {
        return 0;
    }
    char *read = buffer;
    char *write = buffer;
    char *end = buffer + size;

    while (read < end && *read == ' ') {
        read++;
    }
    end--;
    while (end >= read && *end == ' ') {
        end--;
    }
    end++;
    while (read < end) {
        *write++ = *read++;
    }
    return write-buffer;
}

FENNEL_END_CPPFILE("$Id$");

// End FlatFileParser.cpp
