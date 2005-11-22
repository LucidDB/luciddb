/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
// Portions Copyright (C) 2004-2005 John V. Sichi
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

#ifndef Fennel_FlatFileExecStream_Included
#define Fennel_FlatFileExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"

FENNEL_BEGIN_NAMESPACE

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
    std::string path;

    /**
     * Delimiter used to separate fields in a row. The delimiter string may
     * either be empty (for no delimiter), a single character, or the escape
     * \t (for tab).
     */
    std::string fieldDelim;

    /**
     * Delimiter used to terminate a row. The delimiter may be a empty, a
     * single character, or one of the escapes \r, \n, or \r\n. The last three
     * escapes are considered to be equivalent and represent either a carriage
     * return, or a line feed, or both.
     */
    std::string rowDelim;

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

    /*
     * Specifies whether to create an error log for errors encountered while
     * processing tuples. Defaults to false. An error log for a flat file has a
     * similar path to the file, but with a ".err" suffix.
     */
    bool logging;

    explicit FlatFileExecStreamParams()
    {
        fieldDelim = ",";
        rowDelim = "\n";
        quoteChar = '"';
        escapeChar = '\\';
        header = true;
        logging = false;
    }
};

/**
 * FlatFileExecStream parses a text file and produces tuples according to a
 * specified column format. Options support a variety of delimiters and
 * quoting styles. Processing errors are optionally logged to a related file.
 *
 * <p>
 *
 * This implementation does not follow through with conversion of text into
 * native Fennel types. Instead, it scans for the locations of columns in a
 * text buffer and marshalls string values.
 *
 * <p>
 *
 * To actually implement data conversion, a calculator program would be
 * passed in as a parameter to the exec stream. The exec stream would then
 * be able to use a calculator to perform conversions. At the cost of
 * transferring to and from a temporary buffer, all of the conversions
 * could also be handled in a separate CalcExecStream.
 *
 * <p>
 *
 * The actual implementation is in FlatFileExecStreamImpl.
 *
 * @author jpham
 * @version $Id$
 */
class FlatFileExecStream : public SingleOutputExecStream
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
