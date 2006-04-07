/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
#include "fennel/common/FennelExcn.h"
#include "fennel/common/FennelResource.h"
#include "fennel/lucidera/flatfile/FlatFileBinding.h"

FENNEL_BEGIN_CPPFILE("$Id$");

FlatFileBinding::FlatFileBinding(std::string &path, char *buf, uint size) 
{
    this->path = path;
    pBuffer = buf;
    bufferSize = size;
}

PBuffer FlatFileBinding::getBuffer() const
{
    return (PBuffer) pBuffer;
}

uint FlatFileBinding::getBufferSize() const
{
    return bufferSize;
}

void FlatFileBinding::notifyTransferCompletion(bool bSuccess)
{
    if (!bSuccess) {
        throw FennelExcn(
            FennelResource::instance().dataTransferFailed(
                path, bufferSize));
    }
}

FENNEL_END_CPPFILE("$Id$");

// End FlatFileBinding.cpp
