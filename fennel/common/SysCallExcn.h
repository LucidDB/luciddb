/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

#ifndef Fennel_SysCallExcn_Included
#define Fennel_SysCallExcn_Included

#include "fennel/common/FennelExcn.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Exception class for failed system calls.
 */
class SysCallExcn : public FennelExcn
{
private:
    int errCode;

    void init();

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

    /**
     * Constructs a new SysCallExcn.  This may be deferred until some time
     * after the failed system call, as long as the OS error code has been
     * saved.
     *
     * @param msgInit a description of the failure from the program's point of
     * view; SysCallExcn will append additional information from the OS
     *
     * @param errCodeInit OS error code used to generate additional 
     * information
     */
    explicit SysCallExcn(std::string msgInit, int errCodeInit);

    /**
     * Returns the error code that caused this SysCallExcn.
     */
    int getErrorCode();

    /**
     * Returns the current OS error code.  This function may be used to
     * retrieve an error code for use with the 2 argument constructor.
     * The function should be called immediately after the failed system
     * call in order to get the correct information from the OS.
     *
     * @return the current OS error code
     */
    static int getCurrentErrorCode();
};

FENNEL_END_NAMESPACE

#endif

// End SysCallExcn.h
