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

#include "FlatFileParser.h"

FENNEL_BEGIN_CPPFILE("$Id$");

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
    uint maxColumns;
    if (bounded) {
        maxColumns = columns.size();
        result.offsets.resize(maxColumns);
        result.sizes.resize(maxColumns);
    } else {
        maxColumns = FlatFileRowDescriptor::MAX_COLUMNS;
        result.offsets.clear();
        result.sizes.clear();
    }
    bool done = false;
    for (uint i=0; i < maxColumns; i++) {
        int maxLength;
        if (bounded) {
            maxLength = columns[i].maxLength;
        } else {
            maxLength = FlatFileRowDescriptor::MAX_COLUMN_LENGTH;
        }
        scanColumn(
            row + offset,
            size - offset,
            maxLength,
            columnResult);
        switch (columnResult.type) {
        case FlatFileColumnParseResult::NO_DELIM:
            // NOTE: we stop scanning at maximum column length, which is
            // smaller than a page, so we never hit the "large row" error
            result.status = FlatFileRowParseResult::INCOMPLETE_COLUMN;
            done = true;
            break;
        case FlatFileColumnParseResult::ROW_DELIM:
            if (bounded && (i+1 != columns.size())) {
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
            if (bounded && (i+1 == columns.size())) {
                result.status = FlatFileRowParseResult::TOO_MANY_COLUMNS;
                done = true;
            }
            break;
        default:
            permAssert(false);
        }
        if (bounded) {
            result.offsets[i] = offset;
            result.sizes[i] = columnResult.size;
        } else {
            result.offsets.push_back(offset);
            result.sizes.push_back(columnResult.size);            
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
    assert (size >= 0);
    assert (maxLength > 0);
    const char *read = buffer;
    const char *end = buffer + size;
    bool quoted = (size > 0 && *buffer == quote);
    bool quoteEscape = (quoted && quote == escape);
    uint remaining = maxLength;

    result.type = FlatFileColumnParseResult::NO_DELIM;
    if (quoted) {
        read++;
    }
    while (read < end) {
        if (quoteEscape && *read == quote) {
            read++;
            if (read == end) {
                result.type = FlatFileColumnParseResult::NO_DELIM;
                break;
            } else if (*read == quote) {
                // two consecutive quote/escape characters is an escaped quote
                remaining--;
                read++;
            } else {
                // previous character was close quote
                quoted = quoteEscape = false;
            }
        } else if (quoted && *read == quote) {
            quoted = false;
            read++;
        } else if (quoted) {
            read++;
        } else if (*read == fieldDelim) {
            result.type = FlatFileColumnParseResult::FIELD_DELIM;
            break;
        } else if (isRowDelim(*read)) {
            result.type = FlatFileColumnParseResult::ROW_DELIM;
            break;
        } else if (*read == escape) {
            read++;
            if (read == end) {
                result.type = FlatFileColumnParseResult::NO_DELIM;
                break;
            } else {
                remaining--;
                read++;
            }
        } else {
            remaining--;
            read++;
        }
        if (remaining == 0) {
            // try to resolve delimiter for max length field
            if (read < end) {
                if (*read == fieldDelim) {
                    result.type = FlatFileColumnParseResult::FIELD_DELIM;
                } else if (isRowDelim(*read)) {
                    result.type = FlatFileColumnParseResult::ROW_DELIM;
                } else {
                    result.type = FlatFileColumnParseResult::MAX_LENGTH;
                }
            } else {
                result.type = FlatFileColumnParseResult::NO_DELIM;
            }
            break;
        }
    }
        
    switch (result.type) {
    case FlatFileColumnParseResult::NO_DELIM:
    case FlatFileColumnParseResult::MAX_LENGTH:
        assert(read >= buffer);
        result.size = read - buffer;
        result.next = const_cast<char *>(read);
        break;
    case FlatFileColumnParseResult::FIELD_DELIM:
    case FlatFileColumnParseResult::ROW_DELIM:
        result.size = read - buffer;
        result.next = const_cast<char *>(read + 1);
        break;
    default:
        permAssert(false);
    }
}

uint FlatFileParser::stripQuoting(
    char *buffer, uint sizeIn, bool untrimmed) 
{
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
