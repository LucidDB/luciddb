/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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
#include "fennel/disruptivetech/calc/ExtendedInstructionTable.h"
#include "fennel/disruptivetech/calc/ExtendedInstructionContext.h"
#include "fennel/disruptivetech/calc/SqlRegExp.h"

FENNEL_BEGIN_NAMESPACE


// Context for storing cached and reusable pattern and regexp
class ExtRegExpContext : public ExtendedInstructionContext
{
public:
    explicit
    ExtRegExpContext(boost::regex const & re,
                     string const pat) :
        regex(re),
        pattern(pat)
    {
    }
    boost::regex regex;
    string pattern;
};


void
strLikeEscapeA(boost::scoped_ptr<ExtendedInstructionContext>& context,
               RegisterRef<bool>* result,
               RegisterRef<char*>* matchValue,
               RegisterRef<char*>* pattern,
               RegisterRef<char*>* escape) // may be NULL if called by strLikeA
{
    assert(StandardTypeDescriptor::isTextArray(matchValue->type()));
    assert(StandardTypeDescriptor::isTextArray(pattern->type()));

    // SQL99 Part 2 Section 8.5 General Rule 3.a, cases i & ii
    if (matchValue->isNull() ||
        pattern->isNull() ||
        (escape ? escape->isNull() : false)) {
        result->toNull();
        result->length(0);
    } else {
        boost::regex* regexP;
        string* patP;
        ExtRegExpContext* ctxP;

        ctxP = static_cast<ExtRegExpContext*>(context.get());
        if (!ctxP) {
            string pat;
            SqlLikePrep<1,1>(pattern->pointer(),
                             pattern->length(),
                             (escape ? escape->pointer() : 0),
                             (escape ? escape->length() : 0),
                             pat);
            try {
                boost::regex regex(pat);
                context.reset(new ExtRegExpContext(regex, pat));
            }
            catch (boost::bad_expression badexp) {
                // SQL99 Part 2 Section 8.5 General Rule 3.b.i2 *seems* like
                // best fit here.
                // Data Exception - Invalid Escape Sequence
                throw "22025";
            }
            // get context anew
            ctxP = static_cast<ExtRegExpContext*>(context.get());
        }
        regexP = &(ctxP->regex);
        patP = &(ctxP->pattern);

        result->value(SqlRegExp<1,1>(matchValue->pointer(),
                                     matchValue->length(),
                                     pattern->length(),
                                     *regexP));
    }
}

void
strLikeA(boost::scoped_ptr<ExtendedInstructionContext>& context,
         RegisterRef<bool>* result,
         RegisterRef<char*>* matchValue,
         RegisterRef<char*>* pattern)
{
    strLikeEscapeA(context, result, matchValue, pattern, 0);
}


// escape may be NULL if called by strSimilarA
void
strSimilarEscapeA(boost::scoped_ptr<ExtendedInstructionContext>& context,
                  RegisterRef<bool>* result,
                  RegisterRef<char*>* matchValue,
                  RegisterRef<char*>* pattern,
                  RegisterRef<char*>* escape)
{
    assert(StandardTypeDescriptor::isTextArray(matchValue->type()));
    assert(StandardTypeDescriptor::isTextArray(pattern->type()));

    // SQL2003 Part 2 Section 8.5 General Rule 4.a,b
    if (matchValue->isNull() ||
        pattern->isNull() ||
        (escape ? escape->isNull() : false)) {
        result->toNull();
        result->length(0);
    } else {
        boost::regex* regexP;
        string* patP;
        ExtRegExpContext* ctxP;

        ctxP = static_cast<ExtRegExpContext*>(context.get());
        if (!ctxP) {
            string pat;
            SqlSimilarPrep<1,1>(pattern->pointer(),
                                pattern->length(),
                                (escape ? escape->pointer() : 0),
                                (escape ? escape->length() : 0),
                                pat);
            try {
                boost::regex regex(pat);
                context.reset(new ExtRegExpContext(regex, pat));
            }
            catch (boost::bad_expression badexp) {
                // SQL2003 Part 2 Section 8.6 General Rule 2
                // Data Exception - Invalid Regular Expression
                throw "2201B";
            }
            // get context anew
            ctxP = static_cast<ExtRegExpContext*>(context.get());
        }
        regexP = &(ctxP->regex);
        patP = &(ctxP->pattern);

        result->value(SqlRegExp<1,1>(matchValue->pointer(),
                                     matchValue->length(),
                                     pattern->length(),
                                     *regexP));
    }
}

void
strSimilarA(boost::scoped_ptr<ExtendedInstructionContext>& context,
            RegisterRef<bool>* result,
            RegisterRef<char*>* matchValue,
            RegisterRef<char*>* pattern)
{
    strSimilarEscapeA(context, result, matchValue, pattern, 0);
}

void
ExtRegExpRegister(ExtendedInstructionTable* eit)
{
    assert(eit != NULL);

    // JK 2004/5/27: Are all of these combinations really needed?
    int i;
    for (i=0; i < 8; i++) {
        vector<StandardTypeDescriptorOrdinal> params;

        params.push_back(STANDARD_TYPE_BOOL);

        if (i & 0x01) {
            params.push_back(STANDARD_TYPE_CHAR);
        } else {
            params.push_back(STANDARD_TYPE_VARCHAR);
        }
        if (i & 0x02) {
            params.push_back(STANDARD_TYPE_CHAR);
        } else {
            params.push_back(STANDARD_TYPE_VARCHAR);
        }

        eit->add("strLikeA3", params,
                 (ExtendedInstruction3Context<bool, char*, char*>*) NULL,
                 &strLikeA);
        eit->add("strSimilarA3", params,
                 (ExtendedInstruction3Context<bool, char*, char*>*) NULL,
                 &strSimilarA);

        // tack on escape parameter
        if (i & 0x04) {
            params.push_back(STANDARD_TYPE_CHAR);
        } else {
            params.push_back(STANDARD_TYPE_VARCHAR);
        }

        eit->add("strLikeA4", params,
                 (ExtendedInstruction4Context<bool, char*, char*, char*>*) NULL,
                 &strLikeEscapeA);
        eit->add("strSimilarA4", params,
                 (ExtendedInstruction4Context<bool, char*, char*, char*>*) NULL,
                 &strSimilarEscapeA);

    }
}


FENNEL_END_NAMESPACE

// End ExtRegExp.cpp
