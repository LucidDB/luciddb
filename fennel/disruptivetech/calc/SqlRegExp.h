/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
// SqlRegExp
//
// An ASCII & UCS2 string library that adheres to the SQL99 and/or
// SQL2003 standard definitions, and implements LIKE and SIMILAR.
*/
#ifndef Fennel_SqlRegExp_Included
#define Fennel_SqlRegExp_Included

#include <string>
#include <boost/regex.hpp>

#ifdef HAVE_ICU
#include <unicode/ustring.h>
#endif

FENNEL_BEGIN_NAMESPACE

#if !(defined LITTLEENDIAN || defined BIGENDIAN)
#error "endian not defined"
#endif

/** \file SqlRegExp.h 
 *
 * SqlRegExp is a library of string fuctions that perform according to
 * the SQL99 standard and implement the LIKE and SIMILAR operators.
 *
 * These functions are called by ExtendedInstructions in ExtRegExp.h
 *
 * See also file SqlString.h
 *
 */


//! StrLikePrep. Prepares a pattern string to feed to regex
//! and perhaps also ICU's regex.
//!
//! See SQL99 Part 2 Section 8.5
//!
//! Set escape and escapeLenBytes to 0 if escape character is not defined.
//!
//! May throw "22019" or "22025".
//!
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
void
SqlLikePrep(char const * const pattern,
            int patternLenBytes,
            char const * const escape,  // may be null
            int escapeLenBytes,
            std::string& expPat)
{
    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint) {
        if (CodeUnitBytes == 1) {
            // ASCII

            if (patternLenBytes == 0) {
                // SQL99 Part 2 Section 8.5 General Rule 3.d.i.
                // LIKE always matches if matchValueLenBytes == 0
                // if != 0, then I believe this cannot match anything
                // Must still assign a valid regex here.
                expPat.assign("UNUSED");
                return;
            }

            bool escapeIsRegexpSpecial = false;
            std::string special("_%.|*?+(){}[]^$\\");
            char escapeChar;
            if (escapeLenBytes == 1) {
                escapeChar = *escape;
                if (special.find(escapeChar) != std::string::npos &&
                    escapeChar != '_' &&
                    escapeChar != '%') {
                    // escape char is a special char to regex (not
                    // sql, just regex) and must be escaped if it
                    // makes it through to the pattern. (e.g.: escape
                    // = '*', pattern = '**' then regex should be fed
                    // '\*'
                    escapeIsRegexpSpecial = true;
                }
                special.append(1, escapeChar);
            } else {
                if (!escape & !escapeLenBytes) {
                    // Default to no escape character
                    escapeChar = 0; // should not match anything
                } else {
                    // SQL99 Part 2 Section 8.5 General Rule 3.b.i1
                    // Invalid Escape Character
                    throw "22019";
                }
            }
            
            expPat.assign(pattern, patternLenBytes);
            
            // Escape all of ".", "|", "*", "?", "+",
            //        "(", ")", "{", "}", "[", "]", "^", "$", and "\"
            //        so they have no meaning to regex.
            // Convert pat from SQL to Posix (or Perl, tbd) RegExp
            //         _ -> .
            //         % -> .*
            size_t pos = 0;
            while ((pos = expPat.find_first_of(special, pos)) !=
                   std::string::npos) {
                if (expPat[pos] == escapeChar) {
                    if (pos + 1 >= expPat.size() ||
                        (expPat[pos+1] != '_' && expPat[pos+1] != '%' &&
                         expPat[pos+1] != escapeChar)) {
                        // SQL99 Part 2 Section 8.5 General Rule 3.d.ii, I think.
                        // Invalid Escape Sequence
                        throw "22025";
                    }
                    if (escapeIsRegexpSpecial && 
                        expPat[pos+1] == escapeChar) {
                        expPat[pos] = '\\'; // replace escape char 
                        pos+=2;             // move past subsequent escape char
                    } else {
                        expPat.erase(pos, 1); // remove escape char
                        pos++;               // move past subsequent '_' or '%'
                    }
                } else {
                    switch(expPat[pos]) {
                    case '_':   // SQL '_' -> regex '.'
                        expPat.replace(pos, 1, ".");
                        pos++;
                        break;
                    case '%':   // SQL '%' -> regex '.*'
                        expPat.replace(pos, 1, ".*");
                        pos += 2;
                        break;
                    case '\\':
                        // \ is not a special character in LIKE, but
                        // it must be escaped from regex.  Is treated
                        // specially only if it is *not* the escape
                        // char. Note that this also has the side
                        // effect of turning off various character
                        // escape sequences (e.g. \n) and operators
                        // (e.g. \w) that regex supports.
                        expPat.insert(pos, "\\", 1);
                        pos += 2;
                        break;

                    default:    // escape regex special chars
                        // A single \ is the regex escape char
                        expPat.insert(pos, "\\", 1);
                        pos += 2;
                    }
                }
            }
            
        } else if (CodeUnitBytes == 2) {
            // TODO: Add UCS2 here
            // Convert pattern to ICU regex pattern
            throw std::logic_error("no UCS2");
        } else {
            throw std::logic_error("no such encoding");
        }
    } else {
        throw std::logic_error("no UTF8/16/32");
    }
}

//! StrSimilarPrepEscapeProcessing
//! helper to StrSimilarPrep
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
void
SqlSimilarPrepEscapeProcessing(char const * const escape,
                               int escapeLenBytes,
                               char& escapeChar,
                               std::string const & expPat,
                               std::string& sqlSpecial)
{
    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint &&
        CodeUnitBytes == 1) {
        // ASCII

        if (escapeLenBytes == 1) {
            escapeChar = *escape;
            sqlSpecial.append(1, escapeChar);
        
            // Define special characters for SQL2003 Part 2 Section 8.6 General
            // Rule 3.b. (See also Syntax Rule 6.)  Added <right brace> to these
            // list as it appears at first glance to be an omission from the
            // rules.  Could easily be wrong though. There could be a subtle
            // reason why '}' is omitted from these rules
            char const * const SqlSimilarPrepGeneralRule3b = "[]()|^-+*_%?{}";

            if (strchr(SqlSimilarPrepGeneralRule3b, escapeChar)) {
                // Escape char is special char. Must not be
                // present in pattern unless it part of a 
                // correctly formed <escape character>
                size_t pos = 0;
                while ((pos = expPat.find(escapeChar, pos)) !=
                       std::string::npos) {
                    if (pos + 1 >= expPat.size() ||
                        !strchr(SqlSimilarPrepGeneralRule3b,
                                expPat[pos + 1])) {
                        // SQL2003 Part 2 Section 8.6 General Rule 3.b
                        // Data Exception - Invalid Use of Escape Character
                        throw "2200C";
                    }
                    pos += 2; // skip by <escape><special char>
                }
            }
            if (escapeChar == ':' &&
                ((expPat.find("[:") != std::string::npos ||
                  expPat.find(":]") != std::string::npos))) {
                // SQL2003 Part 2 Section 8.6 General Rule 3.c
                // Data Exception -- Escape Character Conflict
                throw "2200B";
            }
        } else {
            if (!escape & ! escapeLenBytes) {
                // Default to no escape character
                escapeChar = 0; // should not match anything
            } else {
                // SQL2003 Part 2 Section 8.6 General Rule 3,
                // Invalid Escape Character
                throw "22019";
            }
        }
    }
}


// StrSimilarPrepRewriteCharEnumeration 
// Helper to StrSimilarPrepReWrite -
// Changes regular character set identifier strings (e.g.: [:ALPHA:])
// into corresponding regex strings.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
void
SqlSimilarPrepRewriteCharEnumeration(std::string& expPat,
                                     size_t& pos,
                                     char const * const SqlSimilarPrepSyntaxRule6,
                                     char escapeChar)
{
    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint &&
        CodeUnitBytes == 1) {
        // ASCII

        // If a <character enumeration> contains a <regular character
        // set identifier> it must be either [[:foo:]] or
        // [^[:foo:]]. All other patterns containing a <regular
        // character set identifier>, e.g. [abc[:foo:]], are assumed
        // (by my reading of the BNF) to be ill-formed.
        if (!expPat.compare(pos, 3, "^[:")) {
            // skip past ^ and process as a usual (e.g. as [:foo:])
            pos++;
        } else if (!expPat.compare(pos, 2, "[:")) {
            // no-op
        } else {
            // The <character enumeration> does not contain a 
            // <regular character set identifier>.
            //
            // SQL2003 Part 2 Section 8.6 Syntax Rule 5 and Syntax Rule 6.  Only
            // <escaped character> and <non-escaped character> are
            // legal between [ and ]. i.e. Unescaped special
            // characters not allowed in <character enumeration>.
            // Of course the exception is '-' <minus sign> and '^'
            // <circumflex>.

            std::string syntaxRule6ForCharEnum("[]()|+*_%?{}");
            syntaxRule6ForCharEnum.append(escapeChar, 1);

            size_t pos2 = pos;
            while ((pos2 = expPat.find_first_of(syntaxRule6ForCharEnum, pos2))
                   != std::string::npos) {
                if (expPat[pos2] == escapeChar) {
                    // skip over next char, assume that it is special
                    pos2 += 2;
                } else if (expPat[pos2] == ']') {
                    // no more special chars. Set is OK
                    break;
                } else {
                    // A special char (as defined by Syntax Rule 6) found
                    // unescaped inside character enumeration 
                    //
                    // SQL2003 Part 2 Section 8.6 General Rule 2
                    // Data Exception - Invalid Regular Expression
                    throw "2201B";
                }
            }
            return;
        }

        //
        // Continue with <regular character set identifier> processing
        //

        // SQL2003 Part 2 Section 8.6 Syntax Rule 3
        // and 8.6 BNF <character enumeration>
        // Must make a few substitutions as regex doesn't match
        // SIMILAR. Also, regex doesn't use [:ALPHA:], only [:alpha:].
        // See General Rule 7.m - 7.s
        //
        // SQL2003 Part 2 Section 8.6 General Rule 7.r, Note 189 refers to
        // SQL2003 3.1.6.42: Whitespace is defined as:
        // U+0009, Horizontal Tabulation
        // U+000A, Line Feed
        // U+000B, Vertical Tabulation
        // U+000C, Form Feed
        // U+000D, Carriage Return
        // U+0085, Next Line
        // Plus Unicode General Category classes Zs, Zl, Zp:
        // (see NOTE 6 & NOTE 7), ignoring those > U+00FF
        // U+0020, Space
        // U+00A0, No-Break Space
        //
        // Table below assumes that only [[:foo:]] and [^[:foo:]] are
        // legal. All other patterns, e.g. [abc[:foo:]] are assumed
        // (by my reading of the BNF) to be ill-formed.
        //
        // TODO: Move this table to .cpp file.
        char const * const regCharSetIdent[][2] = {
            { "[:ALPHA:]", "[:alpha:]" },
            { "[:alpha:]", "[:alpha:]" },
            { "[:UPPER:]", "[:upper:]" },
            { "[:upper:]", "[:upper:]" },
            { "[:LOWER:]", "[:lower:]" },
            { "[:lower:]", "[:lower:]" },
            { "[:DIGIT:]", "[:digit:]" },
            { "[:digit:]", "[:digit:]" },
            { "[:SPACE:]", " " },
            { "[:space:]", " " },
            { "[:WHITESPACE:]", "\x20\xa0\x09\x0a\x0b\x0c\x0d\x85" },
            { "[:whitespace:]", "\x20\xa0\x09\x0a\x0b\x0c\x0d\x85" },
            { "[:ALNUM:]", "[:alnum:]" },
            { "[:alnum:]", "[:alnum:]" },
            { "", "" }
        };
        int i, len;
        for (i = 0; *regCharSetIdent[i][0]; i++) {
            len = strlen(regCharSetIdent[i][0]);
            if (!expPat.compare(pos, len, regCharSetIdent[i][0])) {
                expPat.replace(pos, len, regCharSetIdent[i][1]);
                pos += strlen(regCharSetIdent[i][1]);
                return;
            }
        }
        // SQL2003 Part 2 Section 8.6 General Rule 2
        // Data Exception - Invalid Regular Expression
        throw "2201B";
    }
}


// StrSimilarPrepRewrite
// helper to StrSimilarPrep - changes SQL SIMILAR format to Boost::RegEx format
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
void
SqlSimilarPrepReWrite(char escapeChar,
                      std::string& expPat,
                      std::string& sqlSpecial)
{
    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint &&
        CodeUnitBytes == 1) {
        // ASCII

        // Define special characters for SQL2003 Part 2 Section 8.6 Syntax Rule
        // 6 (similar to Section 8.6 General Rule 3.b).
        //
        // Added <right brace> to these list as it appears at first glance to
        // be an omission from the rules.  Could easily be wrong though. There
        // could be a subtle reason why '}' is omitted from these rules
        //
        char const * const SqlSimilarPrepSyntaxRule6 = "[]()|^-+*_%?{}";

        char const * const BoostRegExEscapeChar = "\\";

        // Escape only "\" so it has no meaning to regex.
        // Convert pat from SQL to Posix (or Perl, tbd) RegExp
        //         _ -> .
        //         % -> .*

        size_t pos = 0;
        bool characterEnumeration = false; // e.g. [A-Z]
        while ((pos = expPat.find_first_of(sqlSpecial, pos)) !=
               std::string::npos) {

            if (expPat[pos] == escapeChar) {
                if (pos + 1 >= expPat.size()) {
                    // Escape char at end of string. See large note above
                    // SQL2003 Part 2 Section 8.6 General Rule 2
                    // Data Exception - Invalid Regular Expression
                    throw "2201B";
                }
                if (strchr(SqlSimilarPrepSyntaxRule6, expPat[pos + 1])) {
                    // Valid <escaped char>, per SQL2003 Part 2 Section 8.6
                    // Syntax Rule 6.  Replace user defined escape char with
                    // regex escape char.
                    expPat.replace(pos, 1, BoostRegExEscapeChar);
                    // Move past subsequent special character.
                    pos += 2;
                } else if (expPat[pos + 1] == escapeChar) {
                    // By inference, escapeChar is not a special char.
                    // Can let the escape char fall through w/o an regex esc.
                    // Delete one of the two <escape><escape> chars:
                    expPat.erase(pos, 1);
                    // Move past the sole remaining <escape> char:
                    pos++; 
                } else {
                    // Malformed <escaped char>. Attempt to escape a
                    // non special character.  SQL2003 Part 2 Section 8.6 Syntax
                    // Rules 5 & 6, combined with General Rule 2 imply
                    // that if an escape character is not followed by
                    // a special character, then the result does not
                    // have the format of a <regular expression> since
                    // the character is neither an <non-escaped
                    // character> nor an <escaped character>.
                    //
                    // SQL2003 Part 2 Section 8.6 General Rule 2
                    // Data Exception - Invalid Regular Expression
                    throw "2201B";
                }
            } else {
                switch (expPat[pos]) {
                case '[':
                    // See long note above on SR5, SR6 and GR 2: by
                    // the BNF, a non-escaped special character is not
                    // legal inside a character enumeration. Besides,
                    // it doesn't make sense.
                    characterEnumeration = true;
                    pos++;
                    SqlSimilarPrepRewriteCharEnumeration
                        <CodeUnitBytes, MaxCodeUnitsPerCodePoint>
                        (expPat, pos, SqlSimilarPrepSyntaxRule6, escapeChar);
                    break;
                case ']':
                    if (!characterEnumeration) {
                        // Closing ']'  w/o opening ']'
                        // SQL2003 Part 2 Section 8.6 General Rule 2
                        // Data Exception - Invalid Regular Expression
                        throw "2201B";
                    }
                    characterEnumeration = false;
                    pos++;
                    break;
                case '_':   // SQL '_' -> regex '.'
                    expPat.replace(pos, 1, ".");
                    pos++;
                    break;
                case '%':   // SQL '%' -> regex '.*'
                    expPat.replace(pos, 1, ".*");
                    pos += 2;
                    break;
                case '\\': 
                    //
                    // Characters that boost::regex treats as special:
                    // ".|*?+(){}[]^$\\":
                    // Characters boost::regex treats as special, on top of 
                    // SqlSimilarPrepSyntaxRule6:
                    // "\\.$"
                    //
                    // \ is not a special character in SIMILAR, but it
                    // must be escaped from regex.  Is treated
                    // specially only if it is *not* the escape
                    // char. Note that this also has the side effect
                    // of turning off various character escape
                    // sequences (e.g. \n) and operators (e.g. \w)
                    // that regex supports.
                    expPat.replace(pos, 1, "\\\\");
                    pos += 2;
                    break;
                case '.':
                    // see comment just above for '\\'
                    expPat.replace(pos, 1, "\\.");
                    pos += 2;
                    break;
                case '$':
                    // see comment just above for '\\'
                    expPat.replace(pos, 1, "\\$");
                    pos += 2;
                    break;
                default:
                    throw std::logic_error("SqlSimilarPrep:escapeSwitch");
                }
            }
        } // while()

        if (characterEnumeration) {
            // Opening '[' w/o closing ']'
            // SQL2003 Part 2 Section 8.6 General Rule 2
            // Data Exception - Invalid Regular Expression
            throw "2201B";
        }
    }
}

//! StrSimilarPrep. Prepares a pattern string to feed to regex
//! and perhaps also ICU's regex.
//!
//! Set escape and escapeLenBytes to 0 if escape character is not defined.
//!
//! May throw "2200B" "22019", "2201B", or "2200C"
//!
//! See SQL99 Part 2 Section 8.6 and SQL2003 Part 2 Section 8.6. This routine adheres to SQL2003
//! as published in the working draft, except as noted below:
//! 
//! Does not support  General Rule 7L which
//! allows the definition of both included and excluded characters
//! from sets at the same time. e.g.: [abc^def]. Seems to be of low value.
//! TODO: Add support for General Rule 7L?
//! TODO: Add support For Note 190: Handling of blanks
//! at the end of the pattern.
//! TODO: Understand and implement General Rule 7.t. (Confused.)
//!
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
void
SqlSimilarPrep(char const * const pattern,
               int patternLenBytes,
               char const * const escape,  // may be null
               int escapeLenBytes,
               std::string& expPat)
{
    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint &&
        CodeUnitBytes == 1) {
        // ASCII

        if (patternLenBytes == 0) {
            // SQL99 and SQL2003 Part 2 Section 8.6 General Rule 2 may
            // come into play here if boost::regex doesn't
            // handle this case properly. Also see
            // SQL2003 Part 2 Section 8.6 General Rule 7.u.
            expPat.assign("UNUSED");
            return;
        }

        expPat.assign(pattern, patternLenBytes);

        // Chars that have different meanings in SIMILAR & regex
        // Note that \\ becomes \ in the string.
        std::string sqlSpecial("\\.$_%[]");
        char escapeChar;

        SqlSimilarPrepEscapeProcessing
            <CodeUnitBytes, MaxCodeUnitsPerCodePoint>
            (escape,
             escapeLenBytes,
             escapeChar,
             expPat,
             sqlSpecial);

        SqlSimilarPrepReWrite
            <CodeUnitBytes, MaxCodeUnitsPerCodePoint>
            (escapeChar,
             expPat,
             sqlSpecial);

    } else if (CodeUnitBytes == MaxCodeUnitsPerCodePoint &&
               CodeUnitBytes == 2) {
        // TODO: Add UCS2 here
        // Convert pattern to ICU regex pattern.
        //
        // Use of std::string in function signature may have to change
        // when ICU support is added.
        throw std::logic_error("no UCS2");
    } else if (CodeUnitBytes == MaxCodeUnitsPerCodePoint) {
        throw std::logic_error("no such encoding");
    } else {
        throw std::logic_error("no UTF8/16/32");
    }
}

//! SqlRegExp. Execs LIKE and SIMILAR. SQL VARCHAR & CHAR. Ascii. No UCS2 yet.
//!
//! See SQL99 Part 2 Section 8.5 & SQL2003 Part 2 Section 8.6
//!
//! patternLenBytes must be passed in to support
//! SQL99 Part 2 Section 8.5 General Rule 3.d.i, patLen = matchLen = 0.
//!
//! TODO: Function signature will change when unicode is supported
//! TODO: to allow either/or regex and ICU regex to be passed in.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
bool
SqlRegExp(char const * const matchValue,
          int matchValueLenBytes,
          int patternLenBytes,
          const boost::regex& exp)
{
    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint) {
        if (CodeUnitBytes == 1) {
            // ASCII

            if (patternLenBytes == 0) {
                if (matchValueLenBytes == 0) {
                    // SQL99 Part 2 Section 8.5 General Rule 3.d.i.  Not
                    // explicitly defined in SQL2003 Part 2 Section 8.6 for
                    // SIMILAR but let this pass as seems reasonable.
                    return true;
                } else {
                    // Believe that this cannot match anything.
                    // Avoid tussle with regex over empty exp
                    return false;
                }
            }

            bool result;
            try {
                result = boost::regex_match(matchValue,
                                            matchValue + matchValueLenBytes,
                                            exp);
            }
            // TODO: Make this a catch bad_expression or similar
            // TODO: and rethrow a SQL error code.
            catch (...) {
                throw std::logic_error("boost::regex error in SqlLike");
            }
            return result;

        } else if (CodeUnitBytes == 2) {
            // TODO: Add UCS2 here
            throw std::logic_error("no UCS2");
        } else {
            throw std::logic_error("no such encoding");
        }
    } else {
        throw std::logic_error("no UTF8/16/32");
    }
}



FENNEL_END_NAMESPACE

#endif

// End SqlRegExp.h
