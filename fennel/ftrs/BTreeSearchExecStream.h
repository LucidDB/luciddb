/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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

#ifndef Fennel_BTreeSearchExecStream_Included
#define Fennel_BTreeSearchExecStream_Included

#include "fennel/ftrs/BTreeReadExecStream.h"
#include "fennel/exec/ConduitExecStream.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/common/SearchEndpoint.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Structure used to store information about dynamic parameters used in the
 * btree search
 */
struct FENNEL_FTRS_EXPORT BTreeSearchKeyParameter
{
    /**
     * Dynamic parameter id
     */
    DynamicParamId dynamicParamId;

    /**
     * Offset within the projected search key that this parameter maps to
     */
    uint keyOffset;

    BTreeSearchKeyParameter(
        DynamicParamId id,
        uint offset)
        : dynamicParamId(id),
        keyOffset(offset)
    {
    }
};

/**
 * BTreeSearchExecStreamParams defines parameters for instantiating a
 * BTreeSearchExecStream.
 */
struct FENNEL_FTRS_EXPORT BTreeSearchExecStreamParams
    : public BTreeReadExecStreamParams, public ConduitExecStreamParams
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
     * (May be empty.)
     */
    TupleProjection inputJoinProj;

    /**
     * Projection of single input attribute to be used as a directive
     * controlling interval searches.  If empty, all input keys are
     * interpreted as point searches.
     */
    TupleProjection inputDirectiveProj;

    /**
     * Dynamic parameter ids corresponding to search values, in the case where
     * the search values are not passed in through the input stream
     */
    std::vector<BTreeSearchKeyParameter> searchKeyParams;
};

/**
 * BTreeSearchExecStream reads keys from a child and returns matching tuples in
 * the BTree.  Optionally, values from the input may also be joined to the
 * output (in which case they come before the values read from the BTree).
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_FTRS_EXPORT BTreeSearchExecStream
    : public BTreeReadExecStream, public ConduitExecStream
{
    /**
     * Ordinals of lower/upper bounds within directive tuple.
     */
    enum DirectiveOrdinal {
        LOWER_BOUND_DIRECTIVE = 0,
        UPPER_BOUND_DIRECTIVE = 1
    };

protected:
    TupleProjectionAccessor inputKeyAccessor;
    TupleProjectionAccessor inputJoinAccessor;
    TupleProjectionAccessor readerKeyAccessor;
    TupleProjectionAccessor directiveAccessor;
    TupleProjectionAccessor upperBoundAccessor;
    TupleDescriptor inputKeyDesc, upperBoundDesc;
    TupleData inputKeyData, upperBoundData, readerKeyData, directiveData,
        *pSearchKey;
    bool outerJoin;
    bool preFilterNulls;
    uint nJoinAttributes;
    SearchEndpoint lowerBoundDirective;
    SearchEndpoint upperBoundDirective;
    bool leastUpper;
    std::vector<BTreeSearchKeyParameter> searchKeyParams;
    boost::scoped_array<FixedBuffer> searchKeyBuffer;
    bool dynamicKeysRead;
    TupleProjection searchKeyProj, upperBoundKeyProj;

    bool innerSearchLoop();
    ExecStreamResult innerFetchLoop(
        ExecStreamQuantum const &quantum,
        uint &nTuples);
    void readDirectives();
    bool testInterval();

    /**
     * Reads the search key either from the input stream or dynamic parameters
     */
    void readSearchKey();

    /**
     * Reads the upper bound key either from the input stream or dynamic
     * parameters
     */
    void readUpperBoundKey();

    /**
     * Determines if the next key value is within the upper bound search
     * range
     *
     * @return true if next key value is within upper bound search range
     */
    bool checkNextKey();

    /**
     * Determines if enough tuples have been produced for this stream
     *
     * @param nTuples number of tuples produced thus far
     *
     * @return true if tuple limit reached
     */
    virtual bool reachedTupleLimit(uint nTuples);

    /**
     * Searches the btree for a specific key.
     *
     * @return true if the search yielded matching keys
     */
    bool searchForKey();

public:
    // implement ExecStream
    void prepare(BTreeSearchExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End BTreeSearchExecStream.h
