/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
#ifndef Fennel_CalcMessage_Included
#define Fennel_CalcMessage_Included

#include "fennel/calc/CalcTypedefs.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Object for passing warning and error messages from the Compiler
 */
class CalcMessage 
{
public:
    explicit
    CalcMessage(const char* str, TProgramCounter pc) 
        : mStr(str), mPc(pc)
    { }

    const char* mStr;
    TProgramCounter mPc;
};

FENNEL_END_NAMESPACE

#endif

// End CalcMessage.h

