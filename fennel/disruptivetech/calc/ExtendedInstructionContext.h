/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 The Eigenbase Project
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

#ifndef Fennel_ExtendedInstructionContext_Included
#define Fennel_ExtendedInstructionContext_Included

FENNEL_BEGIN_NAMESPACE

//! A abstract base class for Extended Instructions that wish to store context
//! between exec() calls. Typically used to store results of pre-compilation
//! or cache instantiations of library classes, and so forth.
//! An alternate implementation could store context pointers in local variables.
class ExtendedInstructionContext
{
public:
    explicit
    ExtendedInstructionContext()
    {
    }
    virtual
    ~ExtendedInstructionContext()
    {
    }
};

FENNEL_END_NAMESPACE

#endif
// End ExtendedInstructionContext.h
