/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifndef Fennel_SortingStream_Included
#define Fennel_SortingStream_Included

#include "fennel/xo/BTreeInserter.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"

FENNEL_BEGIN_NAMESPACE

// TODO:  decouple from BTreeInserter

/**
 * SortingStreamParams defines parameters for instantiating a SortingStream.
 * The rootPageId attribute should always be NULL_PAGE_ID, and tupleDesc should
 * be empty.  Note that when distinctness is DUP_DISCARD, the key should
 * normally be the whole tuple to avoid non-determinism with regards to which
 * tuples are discarded.
 */
struct SortingStreamParams : public BTreeInserterParams
{
};

/**
 * SortingStream sorts its input stream according to a parameterized key and
 * returns the sorted data as its output.  The implementation is currently a
 * stupid insertion sort via a BTree, which is totally inappropriate for every
 * real use.
 *
 *<p>
 *
 * TODO:  external mergesort.
 */
class SortingStream : public BTreeInserter
{
    bool sorted;
    
    virtual void closeImpl();
    
public:
    void prepare(SortingStreamParams const &params);
    virtual void open(bool restart);
    virtual TupleDescriptor const &getOutputDesc() const;
    virtual bool writeResultToConsumerBuffer(
        ByteOutputStream &resultOutputStream);
};

FENNEL_END_NAMESPACE

#endif

// End SortingStream.h
