/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/xo/ExecutionStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ExecutionStreamParams::~ExecutionStreamParams()
{}
    
ExecutionStream::ExecutionStream()
{
    pGraph = NULL;
    id = MAXU;
    isOpen = false;
    name = "";
}

void ExecutionStream::closeImpl()
{
    isOpen = false;
}

SharedExecutionStream ExecutionStream::getStreamInput(uint ordinal)
{
    return pGraph->getStreamInput(getStreamId(),ordinal);
}
    
ExecutionStream::~ExecutionStream()
{
}

void ExecutionStream::prepare(ExecutionStreamParams const &params)
{
}
    
void ExecutionStream::open(bool restart)
{
    if (restart) {
        assert(isOpen);
    } else {
        // NOTE: this assertion is bad because in case of multiple
        // inheritance, open can be called twice.  So we rely on the
        // corresponding assertion in TupleStreamGraph instead, unless
        // someone can come up with something better.
#if 0
        assert(!isOpen);
#endif
        isOpen = true;
        needsClose = true;
    }
}

ExecutionStreamId ExecutionStream::getStreamId() const
{
    return id;
}

void ExecutionStream::setName(std::string const &nameIn)
{
    name = nameIn;
}

std::string const &ExecutionStream::getName() const
{
    return name;
}
    
TupleFormat ExecutionStream::getOutputFormat() const
{
    return TUPLE_FORMAT_STANDARD;
}

ByteInputStream &ExecutionStream::getProducerResultStream()
{
    assert(false);
    throw;
}

bool ExecutionStream::writeResultToConsumerBuffer(
    ByteOutputStream &resultOutputStream) 
{
    assert(false);
    throw;
}

ExecutionStream::BufferProvision 
ExecutionStream::getInputBufferRequirement() const
{
    return NO_PROVISION;
}

void *ExecutionStream::getImpl()
{
    // TODO jvs 8-June-2004:  if we ever get a fix for the JNI+dynamic_cast
    // problem, change this to return this
    return NULL;
}

FENNEL_END_CPPFILE("$Id$");

// End ExecutionStream.h
