/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Technologies, Inc.
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
//
// ReturnException
//
// Object for signaling return from calculator
*/
#ifndef Fennel_ReturnException_Included
#define Fennel_ReturnException_Included

FENNEL_BEGIN_NAMESPACE

#include "fennel/calc/Calculator.h"

class ReturnException 
{
public:
    explicit
    ReturnException(TProgramCounter pc) : mPc(pc) 
    { }
    TProgramCounter mPc;  // record PC that caused return
};

FENNEL_END_NAMESPACE

#endif

// End ReturnException.h

