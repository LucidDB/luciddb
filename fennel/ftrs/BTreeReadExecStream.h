/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/

#ifndef Fennel_BTreeReadExecStream_Included
#define Fennel_BTreeReadExecStream_Included

#include "fennel/ftrs/BTreeExecStream.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleProjectionAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeReadExecStreamParams defines parameters for instantiating a
 * BTreeReadExecStream.
 */
struct BTreeReadExecStreamParams : public BTreeExecStreamParams
{
    /**
     * Projection of attributes to be retrieved from BTree (relative to
     * tupleDesc).
     */
    TupleProjection outputProj;
};

/**
 * BTreeReadExecStream is an abstract base class for ExecStream
 * implementations which project a stream of tuples via a BTreeReader.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class BTreeReadExecStream : public BTreeExecStream
{
protected:
    SharedBTreeReader pReader;
    TupleProjectionAccessor projAccessor;
    TupleData tupleData;
    
public:
    // implement ExecStream
    virtual void prepare(BTreeReadExecStreamParams const &params);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End BTreeReadExecStream.h
