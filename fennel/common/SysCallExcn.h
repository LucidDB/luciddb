/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
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

#ifndef Fennel_SysCallExcn_Included
#define Fennel_SysCallExcn_Included

#include "fennel/common/FennelExcn.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Exception class for failed system calls.
 */
class SysCallExcn : public FennelExcn
{
public:
    /**
     * Constructs a new SysCallExcn.  This should be called immediately after
     * the failed system call in order to get the correct information from the
     * OS.
     *
     * @param msgInit a description of the failure from the program's point of
     * view; SysCallExcn will append additional information from the OS
     */
    explicit SysCallExcn(std::string msgInit);
};

FENNEL_END_NAMESPACE

#endif

// End SysCallExcn.h
