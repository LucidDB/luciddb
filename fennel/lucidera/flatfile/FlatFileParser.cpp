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

FlatFileRowParseResult::FlatFileRowParseResult()
{
    reset();
}

void FlatFileRowParseResult::reset()
{
    status = NO_STATUS;
    current = next = NULL;
    //offsets.clear();
    //sizes.;
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

char *FlatFileParser::scanRow(
    FlatFileBuffer &buffer,
    char *rowIn, 
    FlatFileRowDescriptor columns,
    FlatFileRowParseResult &result)
{
    assert(!(buffer.readCompleted() && rowIn > buffer.contentEnd()));
    char *row = rowIn;
    uint size = buffer.buf() + buffer.size() - row;
    uint offset = 0;
    FlatFileColumnParseResult columnResult;

    result.status = FlatFileRowParseResult::NO_STATUS;
    result.offsets.resize(columns.size());
    result.sizes.resize(columns.size());
    bool done = false;
    for (uint i=0; i < columns.size(); i++) {
        scanColumn(
            row + offset,
            size - offset,
            columns[i].maxLength,
            columns[i].isChar,
            columnResult);
        switch (columnResult.type) {
        case FlatFileColumnParseResult::NO_DELIM:
            if (buffer.readCompleted()) {
                result.status = FlatFileRowParseResult::INCOMPLETE_COLUMN;
                done = true;
                break;
            } else if (row==buffer.buf() && buffer.full()) {
                result.status = FlatFileRowParseResult::COLUMN_TOO_LARGE;
                done = true;
                break;
            } else {
                buffer.fill(row);
                row = buffer.buf();
                size = buffer.size();
                continue;
            }
        case FlatFileColumnParseResult::ROW_DELIM:
            if (i+1 != columns.size()) {
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
            if (i+1 == columns.size()) {
                result.status = FlatFileRowParseResult::TOO_MANY_COLUMNS;
                done = true;
            }
            break;
        default:
            permAssert(false);
        }
        result.offsets[i] = offset;
        result.sizes[i] = columnResult.size;
        offset = columnResult.next - row;
        if (done) break;
    }
    result.current = row;
    result.next = columnResult.next;
    return row;
}

char *FlatFileParser::scanRowEnd(
    FlatFileBuffer &buffer,
    FlatFileRowParseResult &result)
{
    char *read = result.next;
    switch (result.status) {
    case FlatFileRowParseResult::INCOMPLETE_COLUMN:
        return read;
    case FlatFileRowParseResult::COLUMN_TOO_LARGE:
    case FlatFileRowParseResult::TOO_MANY_COLUMNS:
        read = scanRowDelim(buffer, read, true);
        if (read == buffer.contentEnd()) {
            assert(buffer.readCompleted());
            return read;
        }
    case FlatFileRowParseResult::NO_STATUS:
    case FlatFileRowParseResult::NO_COLUMN_DELIM:
    case FlatFileRowParseResult::TOO_FEW_COLUMNS:
        read = scanRowDelim(buffer, read, false);
        break;
    default:
        permAssert(false);
    }
    result.nRowDelimsRead++;
    return read;
}

char *FlatFileParser::scanRowDelim(
    FlatFileBuffer &buffer,
    char *current,
    bool search) 
{
    char *read = current;
    char *end = buffer.buf() + buffer.size();
    while (true) {
        if (read < end && (isRowDelim(*read) == search)) {
            break;
        } else if (read < end) {
            read++;
        } else if (read == end && buffer.readCompleted()) {
            break;
        } else {
            permAssert(read == end);
            buffer.fill(read);
            read = buffer.buf();
            end = buffer.buf() + buffer.size();
            continue;
        }
    }
    return read;
}

bool FlatFileParser::isRowDelim(char c)
{
    assert(c != '\r');
    return (rowDelim == '\n') ? (c == '\r' || c == '\n') : (c == rowDelim);
}

void FlatFileParser::scanColumn(
    char *buffer,
    uint size,
    uint maxLength, 
    bool isChar,
    FlatFileColumnParseResult &result)
{
    assert (size > 0);
    assert (maxLength > 0);
    char *read = buffer;
    char *end = buffer + size;
    bool quoted = (isChar && *buffer == quote);
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
        assert(read > buffer);
        result.size = read - buffer;
        result.next = read;
        break;
    case FlatFileColumnParseResult::FIELD_DELIM:
    case FlatFileColumnParseResult::ROW_DELIM:
        result.size = read - buffer;
        result.next = read + 1;
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
