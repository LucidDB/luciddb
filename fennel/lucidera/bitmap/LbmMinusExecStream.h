/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2009 LucidEra, Inc.
// Copyright (C) 2006-2009 The Eigenbase Project
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
class FENNEL_LBM_EXPORT LbmMinusExecStream
    : public LbmBitOpExecStream
{
    /**
     * True if all subtrahends have reached EOS at some point
     */
    bool subtrahendsDone;

    /**
     * True if a new segment needs to be read from the minuend
     */
    bool needToRead;

    /**
     * Current startrid for the minuend
     */
    LcsRid baseRid;

    /**
     * Current byte segment for the minuend
     */
    PBuffer baseByteSeg;

    /**
     * Length of minuend's current byte segment
     */
    uint baseLen;

    /**
     * Minimum rid from amongst the subtrahends
     */
    LcsRid minSubtrahendRid;

    /**
     * The maximum rid that have been read by a subtrahend.  This only
     * applies if the input has keys.
     */
    LcsRid maxSubtrahendRid;

    /**
     * True if a subtrahend needs to be advanced even though all subtrahends
     * are already positioned past the minuend's startrid
     */
    bool advancePending;

    /**
     * The rid that the subtrahend needs to be advanced to when advancePending
     * is true
     */
    LcsRid advanceSubtrahendRid;

    /**
     * The input containing the subtrahend that needs to be advanced
     */
    int advanceSubtrahendInputNo;

    enum MinusInputType {
        UNKNOWN_INPUT = 0,
        EMPTY_INPUT,
        NONEMPTY_INPUT
    };

    /**
     * Field used to detect the special case of empty inputs. When the
     * subtrahends are empty, there is no need to subtract them.
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
     * Number of bits in the bitmap that keeps track of rid values read from
     * the subtrahends.  Note that because the size of the bitmap is smaller
     * than the number of possible rid values, there can be false hits when
     * testing the bitmap.  The larger this value, the more memory required,
     * but the lower the likelihood of false hits.
     */
    static const uint SUBTRAHEND_BITMAP_SIZE = 32768;

    /**
     * Bitmap containing rid values read from the subtrahends.  Only used in
     * the case when the input contains keys.
     */
    boost::dynamic_bitset<> subtrahendBitmap;

    /**
     * True if the subtrahends need to be restarted
     */
    bool needSubtrahendRestart;

    /**
     * Read a byte segment from the minuend input stream.  In the case where
     * the input has keys, tuples are flushed whenever a new key is read.
     *
     * @param currRid the starting rid value of the segment read
     * @param currByteSeg the first byte of the segment read
     * @param currLen the length of the segment read
     *
     * @return EXECRC_YIELD if a segment was successfully read
     */
    ExecStreamResult readMinuendInputAndFlush(
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
     * Advance a single subtrahend to the specified rid.
     *
     * @param inputNo input number of the subtrahend
     * @param rid rid to be advanced to
     *
     * @return EXECRC_YIELD if advance was successful
     */
    ExecStreamResult advanceSingleSubtrahend(int inputNo, LcsRid rid);

    /**
     * Advances all subtrahends to the desired start rid.
     *
     * @param baseRid desired startrid
     *
     * @returns EXECRC_YIELD if able to successfully advance all subtrahends
     */
    ExecStreamResult advanceSubtrahends(LcsRid baseRid);

    /**
     * Performs the minus operation between the minuend and the subtrahends.
     *
     * @param baseRid start rid of the minuend
     * @param baseByteSeg pointer to the first byte of the minuend segment; note
     * that the segment is stored backwards so it needs to be read from right
     * to left
     * @param baseLen length of the minuend segment
     *
     * @return EXECRC_YIELD if able to read data from subtrahends
     */
    ExecStreamResult minusSegments(
        LcsRid baseRid, PBuffer baseByteSeg, uint baseLen);

    /**
     * Determines which subtrahend contains the minimum rid value in its
     * current input stream.
     *
     * @param minInput returns input number corresponding to the subtrahend
     * with the minimum rid input
     *
     * @return EXECRC_YIELD if able to successfully find a subtrahend; else
     * EXECRC_EOS if all subtrahends have reached EOS
     */
    ExecStreamResult findMinInput(int &minInput);

    /**
     * Restarts the subtrahends, as needed, when the input has keys.
     *
     * <p>The pre-condition for a restart is either a change in key value
     * in the input or unordered rid values in the minuend.  Once this
     * pre-condition is met, then two additional criteria are also required --
     * the current minuend must contain rids that overlap with the
     * subtrahends, and the subtrahend is positioned past the current minuend.
     *
     * <p>For a minuend that meets the pre-condition just described, even if
     * it doesn't meet the additional criteria, then the very first segment
     * that follows that does meet the criteria will need a restart.
     * After that restart is done, then no further restart checks are needed
     * until the restart pre-condition is met again.
     *
     * <p>As part of determining whether or not the restart is necessary,
     * we check for overlapping rids.  If there is no overlap, then the minus
     * operation can be skipped.  This skip minus check is always done,
     * provided the input is non-empty.
     *
     * @return true if the minus operation can be bypassed when the input has
     * keys
     */
    bool checkNeedForRestart();

    /**
     * Determines if it's possible to avoid the minus operation for the
     * current segment read from the minuend by using a bitmap that keeps
     * track of subtrahend rids that have been read.
     *
     * @return true if the minus can be skipped
     */
    bool canSkipMinus();

protected:
    // override LbmBitOpStream
    bool produceTuple(TupleData bitmapTuple);

public:
    virtual void prepare(LbmMinusExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End LbmMinusExecStream.h
