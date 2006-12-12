/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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

#ifndef Fennel_LbmMinusExecStream_Included
#define Fennel_LbmMinusExecStream_Included

#include "fennel/lucidera/bitmap/LbmBitOpExecStream.h"
#include "fennel/lucidera/bitmap/LbmSeqSegmentReader.h"
#include "fennel/tuple/TupleDataWithBuffer.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LbmMinusExecStreamParams defines parameters for instantiating a
 * LbmMinusExecStream
 */
struct LbmMinusExecStreamParams : public LbmBitOpExecStreamParams
{
};

/**
 * LbmMinusExecStream is the execution stream that subtracts from the first
 * bitmap input stream, the bitmap streams from the remaining inputs
 *
 * <p>A minus stream is generally used to subtract an ordered bitmap input
 * streams. In a special case, however, the first input (the "minuend") may
 * contain fields in addition to bitmap fields. The additional fields are
 * considered to be "key fields" and are required to be the first fields.
 * Key fields are propagated to the output stream and allow index only scans
 * to return a result set. A side effect is that the minuend input is only
 * partially ordered when using key fields.
 *
 * <p>To support a partially ordered minuend input, the streams to be
 * subtracted (the "subtrahends") are restarted when the minuend is out of
 * order.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class LbmMinusExecStream : public LbmBitOpExecStream
{
    /**
     * True if all children (i.e., non-anchor) inputs have reached EOS
     */
    bool childrenDone;

    /**
     * True if a new segment needs to be read from the anchor
     */
    bool needToRead;

    /**
     * Current startrid for anchor input
     */
    LcsRid baseRid;

    /**
     * Current byte segment for anchor input
     */
    PBuffer baseByteSeg;

    /**
     * Length of anchor input's current byte segment
     */
    uint baseLen;

    /**
     * Minimum rid from amongst the children input
     */
    LcsRid minChildRid;

    /**
     * True if a child input needs to be advanced even though all children
     * are already positioned past the anchor's startrid
     */
    bool advancePending;

    /**
     * The rid that the child input needs to be advanced to when
     * advancePending is true
     */
    LcsRid advanceChildRid;

    /**
     * The input containing the child that needs to be advanced
     */
    int advanceChildInputNo;

    enum MinusInputType {
        UNKNOWN_INPUT = 0,
        EMPTY_INPUT,
        NONEMPTY_INPUT
    };

    /**
     * Field used to detect the special case of empty inputs. When the
     * subtrahend inputs are empty, there is no need to subtract them.
     */
    MinusInputType inputType;

    /**
     * A sequential reader used when the minuend input has keys, which may
     * lead to RIDs being out of order.
     */
    LbmSeqSegmentReader minuendReader;

    /**
     * Whether the previous set of prefix fields is valid
     */
    bool prevTupleValid;

    /**
     * Previous set of prefix fields
     */
    TupleDataWithBuffer prevTuple;

    /**
     * Whether the prefix should be copied before reading more tuples
     */
    bool copyPrefixPending;

    /**
     * Tuple data used to build prefixed output
     */
    TupleData prefixedBitmapTuple;

    /**
     * Read a byte segment from the minuend input stream. The subtrahends 
     * may be restarted under the following conditions:
     *
     * <ul>
     *   <li>The main stream contains prefix columns
     *   <li>The prefix columns changed between the last segment and the
     *     current segment
     * </ul>
     */
    ExecStreamResult readMinuendInputAndRestart(
        LcsRid &currRid, PBuffer &currByteSeg, uint &currLen);

    /**
     * Reads the minuend input as a random sequence of segments
     */
    ExecStreamResult readMinuendInput(
        LcsRid &currRid, PBuffer &currByteSeg, uint &currLen);

    int comparePrefixes();
    void restartSubtrahends();
    void copyPrefix();

    /**
     * Advance a single child input to the specified rid
     *
     * @param inputNo input number of the child
     * @param rid rid to be advanced to
     *
     * @return EXECRC_YIELD if advance was successful
     */
    ExecStreamResult advanceChild(int inputNo, LcsRid rid);

    /**
     * Advances all children input to the desired start rid
     *
     * @param baseRid desired startrid
     *
     * @returns EXECRC_YIELD if able to successfully advance children
     */
    ExecStreamResult advanceChildren(LcsRid baseRid);

    /**
     * Performs the minus operation on the children of the anchor
     *
     * @param baseRid start rid of the anchor
     * @param baseByteSeg pointer to the first byte of the anchor segment; note
     * that the segment is stored backwards so it needs to be read from right
     * to left
     * @param baseLen length of the anchor segment
     *
     * @return EXECRC_YIELD if able to read data from children
     */
    ExecStreamResult minusSegments(
        LcsRid baseRid, PBuffer baseByteSeg, uint baseLen);

    /**
     * Determines which input child contains the minimum rid value in its 
     * current input stream
     *
     * @param minInput returns input number corresponding to the child with
     * the minimum rid input
     *
     * @return EXECRC_YIELD if able to successfully find a child; else
     * EXECRC_EOS if all children have reached EOS
     */
    ExecStreamResult findMinInput(int &minInput);

protected:
    // override LbmBitOpStream
    bool produceTuple(TupleData bitmapTuple);

public:
    explicit LbmMinusExecStream();
    virtual void prepare(LbmMinusExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End LbmMinusExecStream.h
