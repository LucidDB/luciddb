/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 John V. Sichi.
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

#ifndef Fennel_CopyExecStream_Included
#define Fennel_CopyExecStream_Included

#include "fennel/exec/ConduitExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * CopyExecStreamParams defines parameters for CopyExecStream.
 */
struct CopyExecStreamParams : public ConduitExecStreamParams
{
};

/**
 * CopyExecStream is an adapter for converting the output of a BUFPROV_PRODUCER
 * producer stream for use by a BUFPROF_CONSUMER consumer stream.
 * The implementation copies tuples from the producer buffer to the consumer
 * buffer.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class CopyExecStream : public ConduitExecStream
{
public:
    // implement ExecStream
    virtual void prepare(CopyExecStreamParams const &params);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End CopyExecStream.h
