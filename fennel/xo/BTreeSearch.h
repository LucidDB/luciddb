/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
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

#ifndef Fennel_BTreeSearch_Included
#define Fennel_BTreeSearch_Included

#include "fennel/xo/BTreeReadTupleStream.h"
#include "fennel/xo/SingleInputTupleStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeSearchParams defines parameters for instantiating a BTreeSearch.
 */
struct BTreeSearchParams : public BTreeReadTupleStreamParams
{
    /**
     * When true, make up nulls for unmatched rows.
     */
    bool outerJoin;

    /**
     * Projection of attributes to be used as key from input stream.  If empty,
     * use the entire input stream as key.
     */
    TupleProjection inputKeyProj;

    /**
     * Projection of input attributes to be joined to search results in output.
     */
    TupleProjection inputJoinProj;
};

/**
 * BTreeSearch reads keys from a child and returns matching tuples in the
 * BTree.  Optionally, values from the input may also be joined to the output
 * (in which case they come before the values read from the BTree).
 */
class BTreeSearch : public BTreeReadTupleStream, public SingleInputTupleStream
{
protected:
    TupleAccessor inputAccessor;
    TupleProjectionAccessor inputKeyAccessor;
    TupleProjectionAccessor inputJoinAccessor;
    TupleProjectionAccessor readerKeyAccessor;
    TupleDescriptor inputKeyDesc;
    TupleData inputKeyData,readerKeyData;
    bool outerJoin;
    bool preFilterNulls;
    
    virtual void closeImpl();
    
public:
    void prepare(BTreeSearchParams const &params);
    virtual void open(bool restart);
    virtual bool writeResultToConsumerBuffer(
        ByteOutputStream &resultOutputStream);
    virtual BufferProvision getInputBufferRequirement() const;
    virtual TupleDescriptor const &getOutputDesc() const;
};

FENNEL_END_NAMESPACE

#endif

// End BTreeSearch.h
