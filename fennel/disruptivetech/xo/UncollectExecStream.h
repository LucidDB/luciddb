/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 The Eigenbase Project
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

#ifndef Fennel_UncollectExecStream_Included
#define Fennel_UncollectExecStream_Included

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * UncollectExecStreamParams defines parameters for instantiating a
 * UncollectExecStream.
 */
struct UncollectExecStreamParams : public ConduitExecStreamParams
{
    //empty
};

/**
 * Ouputs all tuples that previously has been collected by CollectExecStream
 *
 * @author Wael Chatila
 * @version $Id$
 */
class UncollectExecStream : public ConduitExecStream
{
private:
    TupleData inputTupleData;
    TupleData outputTupleData;
    uint      bytesWritten;
public:
    virtual void prepare(UncollectExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End UncollectExecStream.h
