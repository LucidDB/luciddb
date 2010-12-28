/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 Dynamo BI Corporation
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
#ifndef Fennel_SqlState_Included
#define Fennel_SqlState_Included

#include <map>

FENNEL_BEGIN_NAMESPACE

/** \file SqlState.h
 *
 * SqlState describes the error codes from the calculator.
 * The values of these codes are specified in the SQL:2008 standard.
 */

/**
 * Information about a particular error code.
 */
class FENNEL_CALCULATOR_EXPORT SqlStateInfo {
private:
    std::string _code;

public:
    SqlStateInfo(const char *code);

    std::string str() const;
};

/**
 * Enumeration of the SqlState values defined in the SQL:2008 standard.
 */
class FENNEL_CALCULATOR_EXPORT SqlState
{
public:
    static const SqlState &instance();

    const SqlStateInfo *lookup(const char *code) const;

    /// Cardinality violation
    const SqlStateInfo &code21000() const
    {
        return _code21000;
    }

    const SqlStateInfo &code22000() const
    {
        return _code22000;
    }

    /// SQL99 Part 2 Section 6.22 General Rule 9.a.iii =>
    /// exception SQL99 22.1 22-001 "String Data Right truncation"
    const SqlStateInfo &code22001() const
    {
        return _code22001;
    }

    const SqlStateInfo &code22003() const
    {
        return _code22003;
    }

    const SqlStateInfo &code22004() const
    {
        return _code22004;
    }

    /// Parse of date failed
    /// SQL2003 Part 2 Section 6.12 General Rule 13 data
    /// exception -- invalid datetime format
    const SqlStateInfo &code22007() const
    {
        return _code22007;
    }

    /// SQL2003 Part 2 Section 8.6 General Rule 3.c
    /// Data Exception -- Escape Character Conflict
    const SqlStateInfo &code2200B() const
    {
        return _code2200B;
    }

    /// SQL2003 Part 2 Section 8.6 General Rule 3.b
    /// Data Exception - Invalid Use of Escape Character
    const SqlStateInfo &code2200C() const
    {
        return _code2200C;
    }

    // SQL99 Part 2 Section 6.18 General Rule 3.d,
    // "data exception substring error". SQL99 22.1 22-011
    const SqlStateInfo &code22011() const
    {
        return _code22011;
    }

    const SqlStateInfo &code22012() const
    {
        return _code22012;
    }

    /// SQL99 Part 2 Section 6.22 General Rule 6.b.i data
    /// exception -- invalid character value for cast
    const SqlStateInfo &code22018() const
    {
        return _code22018;
    }

    /// SQL99 Part 2 Section 8.5 General Rule 3.b.i1
    /// Invalid Escape Character
    const SqlStateInfo &code22019() const
    {
        return _code22019;
    }

    /// SQL2003 Part 2 Section 8.6 General Rule 2
    /// Data Exception - Invalid Regular Expression
    const SqlStateInfo &code2201B() const
    {
        return _code2201B;
    }

    /// SQL2003 Part 2 Section 6.27 General Rule 10.b
    /// Data Exception - Invalid Argument For Natural Logarithm
    const SqlStateInfo &code2201E() const
    {
        return _code2201E;
    }

    /// SQL2003 Part 2 Section 6.27 General rule 12.b
    /// Data Exception - Invalid Argument For Power Function
    const SqlStateInfo &code2201F() const
    {
        return _code2201F;
    }

    /// SQL99 Part 2 Section 22.1 22-023 "invalid parameter value"
    const SqlStateInfo &code22023() const
    {
        return _code22023;
    }

    /// SQL99 Part 2 Section 8.5 General Rule 3.b.i2
    /// Data Exception - Invalid Escape Sequence
    const SqlStateInfo &code22025() const
    {
        return _code22025;
    }

    /// SQL99 Part 2 Section 6.18 General Rule 8.d) Data
    /// Exception - Trim Error
    const SqlStateInfo &code22027() const
    {
        return _code22027;
    }

private:
    SqlState();

    static SqlState _instance;
    std::map<std::string, SqlStateInfo *> _map;
    SqlStateInfo _code21000;
    SqlStateInfo _code22000; /* S_UNDR: ?? underflow, S_INEX: ?? inexact */
    SqlStateInfo _code22001;
    SqlStateInfo _code22003; /* S_OVER: NUMERIC_VALUE_OUT_OF_RANGE */
    SqlStateInfo _code22004;
    SqlStateInfo _code22007;
    SqlStateInfo _code2200B;
    SqlStateInfo _code2200C;
    SqlStateInfo _code22011;
    SqlStateInfo _code22012; /* S_DIV0: DIVISION_BY_ZERO */
    SqlStateInfo _code22018;
    SqlStateInfo _code22019;
    SqlStateInfo _code2201B;
    SqlStateInfo _code2201E;
    SqlStateInfo _code2201F;
    SqlStateInfo _code22023; /* S_INVL: INVALID_PARAMETER_VALUE */
    SqlStateInfo _code22025;
    SqlStateInfo _code22027;
};

FENNEL_END_NAMESPACE

#endif

// End SqlState.h
