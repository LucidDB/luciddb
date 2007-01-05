/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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

#ifndef Fennel_DiffluenceExecStream_Included
#define Fennel_DiffluenceExecStream_Included

#include "fennel/exec/SingleInputExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * DiffluenceExecStreamParams defines parameters for DiffluenceExecStream.
 */
struct DiffluenceExecStreamParams : virtual public SingleInputExecStreamParams
{
    /**
     * Output tuple descriptor.  Currently, all outputs must have the same
     * descriptor.
     */
    TupleDescriptor outputTupleDesc;

    TupleFormat outputTupleFormat;

    explicit DiffluenceExecStreamParams();
};
    
/**
 * DiffluenceExecStream is an abstract base for any ExecStream with
 * multiple outputs and exactly one input.
 *
 * @author Rushan Chen
 * @version $Id$
 */
class DiffluenceExecStream : virtual public SingleInputExecStream
{
protected:

    /**
     * List of output buffer accessors.
     */
    std::vector<SharedExecStreamBufAccessor> outAccessors;

    /**
     * Output tuple descriptor.  Currently, all outputs must have the same
     * descriptor.
     */
    TupleDescriptor outputTupleDesc;

public:
    // implement ExecStream
    virtual void prepare(DiffluenceExecStreamParams const &params);
    virtual void setOutputBufAccessors(
        std::vector<SharedExecStreamBufAccessor> const &outAccessors);
    virtual void open(bool restart);
    /**
     * Indicate to the consumer if the buffer is provided by this exec stream
     * which is the producer.
     */
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End DiffluenceExecStream.h
