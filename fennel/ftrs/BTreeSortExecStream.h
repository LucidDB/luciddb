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

#ifndef Fennel_BTreeSortExecStream_Included
#define Fennel_BTreeSortExecStream_Included

#include "fennel/ftrs/BTreeInsertExecStream.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeSortExecStreamParams defines parameters for instantiating a
 * BTreeSortExecStream.  The rootPageId attribute should always be
 * NULL_PAGE_ID.  Note that when distinctness is
 * DUP_DISCARD, the key should normally be the whole tuple to avoid
 * non-determinism with regards to which tuples are discarded.
 */
struct BTreeSortExecStreamParams : public BTreeInsertExecStreamParams
{
};

/**
 * BTreeSortExecStream sorts its input stream according to a parameterized key
 * and returns the sorted data as its output, using a BTree to accomplish the
 * sort.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class BTreeSortExecStream : public BTreeInsertExecStream
{
    bool sorted;
    
    virtual void closeImpl();
    
public:
    // implement ExecStream
    void prepare(BTreeSortExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End BTreeSortExecStream.h
