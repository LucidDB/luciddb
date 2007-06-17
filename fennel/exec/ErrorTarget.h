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

#ifndef Fennel_ErrorTarget_Included
#define Fennel_ErrorTarget_Included

#include "fennel/tuple/TupleDescriptor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Row error severity levels. Keep this consistent with
 * net.sf.farrago.NativeRuntimeContext
 */
enum ErrorLevel 
{
    ROW_ERROR = 1000,
    ROW_WARNING = 500
};

/**
 * ErrorTarget defines an interface for receiving Fennel row errors.
 * Typically, many or all ErrorSouce instances post errors to the same
 * ErrorTarget.
 */
class ErrorTarget 
{
public:

    virtual ~ErrorTarget();

    /**
     * Receives notification when a row exception occurs.
     *
     * @param source the unique Fennel stream name
     *
     * @param level the severity of the exception
     *
     * @param message a description of the exception
     *
     * @param address pointer to the buffer containing the error record
     *
     * @param capacity the size of the error buffer
     *
     * @param index position of the column whose processing caused the 
     *   exception to occur. -1 indicates that no column was culpable. 
     *   0 indicates that a filter condition was being processed. Otherwise 
     *   this parameter should be a 1-indexed column position.
     */
    virtual void notifyError(
        const std::string &source,
        ErrorLevel level,
        const std::string &message,
        void *address,
        long capacity,
        int index) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End ErrorTarget.h
