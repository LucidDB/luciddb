/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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
#include "fennel/exec/ErrorSource.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ErrorSource::ErrorSource()
{
    pErrorTarget.reset();
}

ErrorSource::ErrorSource(
    SharedErrorTarget pErrorTargetInit,
    const std::string &nameInit)
{
    pErrorTarget.reset();
    initErrorSource(pErrorTargetInit, nameInit);
}

ErrorSource::~ErrorSource()
{
    pErrorTarget.reset();
}

void ErrorSource::initErrorSource(
    SharedErrorTarget pErrorTargetInit,
    const std::string &nameInit)
{
    pErrorTarget = pErrorTargetInit;
    name = nameInit;
}

void ErrorSource::postError(
    ErrorLevel level, const std::string &message,
    void *address, long capacity, int index)
{
    if (hasTarget()) {
        getErrorTarget().notifyError(
            name, level, message, address, capacity, index);
    }
}

void ErrorSource::postError(
    ErrorLevel level, const std::string &message,
    const TupleDescriptor &errorDesc, const TupleData &errorTuple, int index)
{
    if (!hasTarget()) {
        return;
    }

    if (!pErrorBuf) {
        errorAccessor.compute(errorDesc);
        uint cbMax = errorAccessor.getMaxByteCount();
        pErrorBuf.reset(new FixedBuffer[cbMax]);
    }

    uint cbTuple = errorAccessor.getByteCount(errorTuple);
    errorAccessor.marshal(errorTuple, pErrorBuf.get());
    postError(level, message, pErrorBuf.get(), cbTuple, index);
}

void ErrorSource::disableTarget()
{
    pErrorTarget.reset();
}

FENNEL_END_CPPFILE("$Id$");

// End ErrorSource.cpp
