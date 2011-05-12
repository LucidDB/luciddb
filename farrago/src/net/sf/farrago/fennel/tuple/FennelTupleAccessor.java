/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package net.sf.farrago.fennel.tuple;

import java.nio.*;

import java.util.*;


// NOTE: see comments on struct StoredNode before modifying the way tuples are
// stored.

/**
 * FennelTupleAccessor defines how to efficiently marshall and unmarshall values
 * in a stored tuple. The same logical tuple definition can have multiple
 * storage formats. See <a href="structTupleDesign.html#FennelTupleAccessor">the
 * design docs</a> for more details. This class is JDK 1.4 compatible.
 */
public final class FennelTupleAccessor
{
    public enum TupleFormat {
        TUPLE_FORMAT_STANDARD,
        TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED,
        TUPLE_FORMAT_NETWORK
    }

    public enum TupleAlignment {
        TUPLE_ALIGN4(4), TUPLE_ALIGN8(8);

        private final int alignment;

        TupleAlignment(int align)
        {
            alignment = align;
        }

        public final int getAlignmentMask()
        {
            return alignment - 1;
        }

        public final int getAlignment()
        {
            return alignment;
        }
    }

    //~ Static fields/initializers ---------------------------------------------

    /**
     * format of the constructed buffer. FIXME: not supported; is this
     * necessary?
    public static final int TUPLE_FORMAT_STANDARD = 0;
    public static final int TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED = 1;
    public static final int TUPLE_FORMAT_NETWORK = 2;
     */

    // stored values offsets are (unsigned) short
    static final int STOREDVALUEOFFSETSIZE = 2;

    /**
     * Specifies 4-byte alignment.
     */
    //public static final int TUPLE_ALIGN4 = 4;

    /**
     * Specifies 8-byte alignment.
     */
    //public static final int TUPLE_ALIGN8 = 8;

    /**
     * Specifies alignment matching the data model of this JVM; fallback is to
     * assume 4-byte if relevant system property is undefined. TODO jvs
     * 26-May-2007: we really ought to be calling Fennel to get this instead,
     * since e.g. on Sun CPU architectures, 64-bit alignment is required even
     * for a 32-bit JVM. Plus this System property is undocumented, although
     * it's also available on JRockit.
     */
    //public static final int TUPLE_ALIGN_JVM =
    //  "64".equals(System.getProperty("sun.arch.data.model")) ? TUPLE_ALIGN8
    //  : TUPLE_ALIGN4;

    //~ Instance fields --------------------------------------------------------

    /**
     * Precomputed accessors for attributes, in logical tuple order.
     */
    private final List attrAccessors = new ArrayList();

    /**
     * Array of 0-based indices of variable-width attributes.
     */
    private final List varWidthAccessors = new ArrayList();

    /**
     * maximum marshalled size of this tuple
     */
    private int maxStorage;

    /**
     * minimum marshalled size of this tuple
     */
    private int minStorage;

    /**
     * Precomputed size of bit field array (in bits).
     */
    private int nBitFields;

    /**
     * Precomputed byte offset for bit array.
     */
    private int bitFieldOffset;

    /**
     * Precomputed offset for indirect offset of end of first variable-width
     * attribute, or Integer.MAX_VALUE if there are no variable-width
     * attributes.
     */
    private int firstVarEndIndirectOffset;

    /**
     * Precomputed offset for indirect offset of end of last variable-width
     * attribute, or Integer.MAX_VALUE if there are no variable-length
     * attributes.
     */
    private int lastVarEndIndirectOffset;

    /**
     * Precomputed offset for fixed start of first variable-width attribute, or
     * Integer.MAX_VALUE if there are no variable-width attributes.
     */
    private int firstVarOffset;

    /**
     * current ByteBuffer used for unmarshalling; set by setCurrentTupleBuf().
     */
    private ByteBuffer currTupleBuf;

    /**
     * actual format of this accessor.
     */
    private TupleFormat format;

    /**
     * tuple byte alignment.
     */
    private TupleAlignment tupleAlignment;

    /**
     * mask derived from tupleAlignment
     */
    private int tupleAlignmentMask;

    /**
     * if true, set the ByteBuffer to native order after slicing, when doing
     * unmarshals
     */
    private final boolean setNativeOrder;

    /**
     * Permutation in which attributes should be marshalled; null when
     * !hasAlignedVar, in which case attributes should be marshalled in logical
     * order.
     */
    private List marshalOrder;

    /**
     * Whether any variable-width attributes with alignment requirements
     * (currently restricted to 2-byte alignment for UNICODE strings) are
     * present.
     */
    private boolean hasAlignedVar;

    //~ Constructors -----------------------------------------------------------

    /**
     * Specifies alignment matching the data model of this JVM; fallback
     * is to assume 4-byte if relevant system property is undefined.
     * TODO jvs
     * 26-May-2007: we really ought to be calling Fennel to get this
     * instead, since e.g. on Sun CPU architectures, 64-bit alignment is
     * required evenfor a 32-bit JVM. Plus this System property is
     * undocumented, although it's also available on JRockit.
     */
    public static TupleAlignment getNativeTupleAlignment()
    {
        if ("64".equals(System.getProperty("sun.arch.data.model"))) {
            return TupleAlignment.TUPLE_ALIGN8;
        }
        return TupleAlignment.TUPLE_ALIGN4;
    }

    /**
     * default construction.
     */
    public FennelTupleAccessor()
    {
        this(false);
    }

    /**
     * Creates tuple accessor with the default byte alignmnent and a flag
     * indicating whether byte ordering should be set to native order after
     * slicing.
     *
     * @param setNativeOrder if true, set byte ordering to native order after
     * slicing
     */
    public FennelTupleAccessor(boolean setNativeOrder)
    {
        this.tupleAlignment = getNativeTupleAlignment();
        tupleAlignmentMask = tupleAlignment.getAlignmentMask();
        this.setNativeOrder = setNativeOrder;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * rounds up a value to the next multiple of {@link #tupleAlignment}.
     */
    public int alignRoundUp(int val)
    {
        int lobits = val & tupleAlignmentMask;
        if (lobits != 0) {
            val += (tupleAlignment.getAlignment() - lobits);
        }
        return val;
    }

    /**
     * gets the number of attributes contained in this accessor.
     */
    public int size()
    {
        return attrAccessors.size();
    }

    /**
     * gets an unsigned short value.
     */
    public static int readUnsignedShort(ByteBuffer buffer, int position)
    {
        short signedShort = buffer.getShort(position);
        return (((int) signedShort) << 16) >>> 16;
    }

    /**
     * Initialize all the fixed width accessors in a tuple.
     */
    private void initFixedAccessors(FennelTupleDescriptor tuple, List list)
    {
        int i;
        for (i = 0; i < list.size(); i++) {
            int iAttr = ((Integer) list.get(i)).intValue();
            FennelTupleAttributeDescriptor attr = tuple.getAttr(iAttr);
            FennelAttributeAccessor accessor = getAttributeAccessor(iAttr);
            accessor.fixedOffset = maxStorage;
            maxStorage += attr.storageSize;
        }
    }

    /**
     * Initialize all the variable width accessors in a tuple.
     */
    private void initVarWidthAccessors(FennelTupleDescriptor tuple, List list)
    {
        int i;
        for (i = 0; i < list.size(); i++) {
            int iAttr = ((Integer) list.get(i)).intValue();
            FennelTupleAttributeDescriptor attr = tuple.getAttr(iAttr);
            FennelAttributeAccessor accessor = getAttributeAccessor(iAttr);
            accessor.endIndirectOffset = maxStorage;
            maxStorage += STOREDVALUEOFFSETSIZE;
        }
    }

    /**
     * Initialize all the bit accessors in a tuple.
     */
    private void initBitFieldAccessors(FennelTupleDescriptor tuple, List list)
    {
        int i;
        for (i = 0; i < list.size(); i++) {
            int iAttr = ((Integer) list.get(i)).intValue();
            FennelTupleAttributeDescriptor attr = tuple.getAttr(iAttr);
            FennelAttributeAccessor accessor = getAttributeAccessor(iAttr);
            accessor.computeBitAccessors(bitFieldOffset);
        }
    }

    /**
     * reset this accessor.
     */
    private void clear()
    {
        attrAccessors.clear();
        varWidthAccessors.clear();
        currTupleBuf = null;
        marshalOrder = null;
        hasAlignedVar = false;
    }

    /**
     * gets an accessor by its numeric position.
     */
    public FennelAttributeAccessor getAccessor(int i)
    {
        return (FennelAttributeAccessor) attrAccessors.get(i);
    }

    /**
     * Precomputes access for a particular tuple format. Must be called before
     * any other method.
     *
     * @param tuple the tuple to be accessed
     * @param format how to store tuple
     * @param align the byte alignement to use on tuple boundaries
     */
    public void compute(
        FennelTupleDescriptor tuple, TupleFormat format,
        TupleAlignment align)
    {
        clear();
        this.format = format;
        tupleAlignment = align;
        tupleAlignmentMask = tupleAlignment.getAlignmentMask();

        // these vectors keep track of the logical 0-based indices of the
        // attributes belonging to the various attribute storage classes
        ArrayList aligned8 = new ArrayList();
        ArrayList aligned4 = new ArrayList();
        ArrayList aligned2 = new ArrayList();
        ArrayList unalignedFixed = new ArrayList();
        ArrayList bitAccessors = new ArrayList();
        ArrayList unalignedVar = new ArrayList();
        ArrayList alignedVar2 = new ArrayList();

        // special-case reference to the accessor for the first variable-width
        // attribute
        FennelAttributeAccessor firstVariableAccessor = null;

        // number of bit fields seen so far
        nBitFields = 0;

        // sum of max storage size for variable-width attributes seen so far
        int varDataMax = 0;

        // sum of total storage size seen so far; this is used as an
        // accumulator for assigning actual offsets
        maxStorage = 0;

        // first pass over all attributes in logical order:  collate them into
        // storage classes and precompute everything we can
        int iAttr;
        for (iAttr = 0; iAttr < tuple.getAttrCount(); iAttr++) {
            FennelAttributeAccessor newAccessor;
            FennelTupleAttributeDescriptor attr = tuple.getAttr(iAttr);

            int fixedSize = attr.typeDescriptor.getFixedByteCount();
            int minSize =
                attr.typeDescriptor.getMinByteCount(
                    attr.storageSize);

            if (fixedSize > 0) {
                assert (fixedSize == attr.storageSize);
                assert (fixedSize == minSize);
            }
            boolean isFixedWidth = (minSize == attr.storageSize);
            if (isFixedWidth && (attr.storageSize == 0)) {
                if (attr.typeDescriptor.getMinByteCount(1) == 0) {
                    // this is a "0-length variable-width" field masquerading
                    // as a fixed-width field
                    isFixedWidth = false;
                }
            }
            boolean isNullable = attr.isNullable;
            int nBits = attr.typeDescriptor.getBitCount();
            assert (nBits <= 1);

            /*
            if (format == TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED) { isFixedWidth =
             true; isNullable = false; nBits = 0; }
             */
            int alignment =
                attr.typeDescriptor.getAlignmentByteCount(
                    attr.storageSize);
            newAccessor = attr.typeDescriptor.newAttributeAccessor();
            if (!isFixedWidth) {
                varDataMax += attr.storageSize;
                if (alignment == 2) {
                    hasAlignedVar = true;
                    alignedVar2.add(new Integer(iAttr));
                } else {
                    assert (alignment == 1);
                    unalignedVar.add(new Integer(iAttr));
                }
            } else if (nBits > 0) {
                newAccessor.valueBitNdx = nBitFields;
                nBitFields++;
            } else {
                // fixed-width accessors
                assert ((minSize % alignment) == 0);
                switch (alignment) {
                case 1:
                    unalignedFixed.add(new Integer(iAttr));
                    break;
                case 2:
                    aligned2.add(new Integer(iAttr));
                    break;
                case 4:
                    aligned4.add(new Integer(iAttr));
                    break;
                case 8:
                    aligned8.add(new Integer(iAttr));
                    break;
                default:
                    unalignedFixed.add(new Integer(iAttr));
                    break;
                }
            }

            /*
             * set nullable
             */
            if (isNullable) {
                newAccessor.nullBitNdx = nBitFields;
                nBitFields++;
            }

            // track which accessors have bitfield offsets
            if (isNullable || (nBits > 0)) {
                bitAccessors.add(new Integer(iAttr));
            }
            newAccessor.capacity = attr.storageSize;
            attrAccessors.add(newAccessor);
        }

        // deal with variable-width attributes
        varWidthAccessors.addAll(alignedVar2);
        varWidthAccessors.addAll(unalignedVar);
        if (!varWidthAccessors.isEmpty()) {
            FennelAttributeAccessor attrAccessor =
                (FennelAttributeAccessor) attrAccessors.get(
                    ((Integer) varWidthAccessors.get(0)).intValue());
            firstVariableAccessor = attrAccessor;
        }

        // now, make a pass over each storage class, calculating actual
        // offsets; note that initFixedAccessors advances maxStorage
        // as a side-effect
        initFixedAccessors(tuple, aligned8);
        initFixedAccessors(tuple, aligned4);
        initFixedAccessors(tuple, aligned2);

        if (firstVariableAccessor != null) {
            firstVarEndIndirectOffset = maxStorage;
            initVarWidthAccessors(tuple, varWidthAccessors);
            lastVarEndIndirectOffset = maxStorage - STOREDVALUEOFFSETSIZE;
        } else {
            firstVarEndIndirectOffset = Integer.MAX_VALUE;
            lastVarEndIndirectOffset = Integer.MAX_VALUE;
        }

        initFixedAccessors(tuple, unalignedFixed);

        if (nBitFields > 0) {
            bitFieldOffset = maxStorage;
            initBitFieldAccessors(tuple, bitAccessors);

            // allocate space, rounding up
            // FIXME: change 'divide' to 'bitshift' once proven
            maxStorage += (7 + nBitFields) / 8;
        } else {
            bitFieldOffset = Integer.MAX_VALUE;
        }
        if (firstVariableAccessor != null) {
            if (hasAlignedVar) {
                // First variable-width value needs to be 2-byte aligned,
                // so add one byte of padding if necessary.
                if ((maxStorage & 1) != 0) {
                    ++maxStorage;
                }
            }
            firstVariableAccessor.fixedOffset = maxStorage;
            firstVarOffset = maxStorage;
        } else {
            firstVarOffset = Integer.MAX_VALUE;
        }
        minStorage = maxStorage;
        maxStorage += varDataMax;

        // Avoid 0-byte tuples, because it's very hard to count something
        // that isn't there.  This bumps them up to 1-byte, which will get
        // further bumped up to the minimum alignment unit below.
        if (maxStorage == 0) {
            minStorage = 1;
            maxStorage = 1;
        }

        // now round the entire row width up to the next alignment boundary;
        // this only affects the end of the row, which is why it is done
        // AFTER computing maxStorage based on the unaligned minStorage
        minStorage = alignRoundUp(minStorage);
        maxStorage = alignRoundUp(maxStorage);

        // if aligned variable-width fields are present, permute the
        // marshalling order so that they come before unaligned
        // variable-width fields
        if (hasAlignedVar) {
            marshalOrder = new ArrayList();

            // add all of the fixed-width attributes
            for (int i = 0; i < attrAccessors.size(); ++i) {
                FennelAttributeAccessor accessor =
                    (FennelAttributeAccessor) attrAccessors.get(i);
                if (accessor.endIndirectOffset == Integer.MAX_VALUE) {
                    marshalOrder.add(new Integer(i));
                }
            }

            // then all of the variable-width attributes, in the correct order
            marshalOrder.addAll(varWidthAccessors);
            assert (marshalOrder.size() == attrAccessors.size());
        }
    }

    /**
     * Precomputes access for a particular tuple format. Must be called before
     * any other method. Assumes the default format parameter.
     *
     * @param tuple the tuple to be accessed
     * @param format how to store tuple
     */
    public void compute(FennelTupleDescriptor tuple, TupleFormat format)
    {
        compute(tuple, format, tupleAlignment);
    }

    /**
     * Precomputes access for a particular tuple format. Must be called before
     * any other method. Assumes the default format parameter.
     *
     * @param tuple the tuple to be accessed
     */
    public void compute(FennelTupleDescriptor tuple)
    {
        compute(tuple, TupleFormat.TUPLE_FORMAT_STANDARD);
    }

    /**
     * returns the maximum possible tuple storage size in bytes
     */
    public int getMaxByteCount()
    {
        return maxStorage;
    }

    /**
     * returns the minimum possible tuple storage size in bytes
     */
    public int getMinByteCount()
    {
        return minStorage;
    }

    /**
     * Indicates whether all tuples will have the same fixed size.
     *
     * <p>NOTE: this is copied from fennel, but what about isNullable fields?
     */
    public boolean isFixedWidth()
    {
        return firstVarOffset == Integer.MAX_VALUE;
    }

    /**
     * gets the offset of the first byte of bit fields, or Integer.MAX_VALUE if
     * no bit fields are present.
     */
    public int getBitFieldOffset()
    {
        return bitFieldOffset;
    }

    /**
     * Accesses the ByteBuffer storing the current tuple image.
     *
     * @return address of tuple image, or NULL if no current tuple
     */
    public ByteBuffer getCurrentTupleBuf()
    {
        return currTupleBuf;
    }

    /**
     * Sets the buffer storing the current tuple image. Must be called before
     * getCurrentByteCount and unmarshal.
     *
     * @param currTupleBuf address of tuple image
     */
    public void setCurrentTupleBuf(ByteBuffer currTupleBuf)
    {
        this.currTupleBuf = currTupleBuf;
    }

    /**
     * Forgets the current tuple buffer.
     */
    public void resetCurrentTupleBuf()
    {
        currTupleBuf = null;
    }

    /**
     * Determines the number of bytes stored in the current tuple buffer. This
     * will always be greater than or equal to getMinByteCount() and less than
     * getMaxByteCount().
     *
     * @return byte count
     */
    public int getCurrentByteCount()
    {
        assert (currTupleBuf != null);
        return getBufferByteCount(currTupleBuf);
    }

    /**
     * Determines the number of bytes stored in a tuple buffer without actually
     * preparing to unmarshal it.
     *
     * @param pBuf tuple buffer
     *
     * @return byte count
     */
    public int getBufferByteCount(ByteBuffer pBuf)
    {
        if (lastVarEndIndirectOffset == Integer.MAX_VALUE) {
            // fixed-width tuple
            return maxStorage;
        }
        int dataLen = readUnsignedShort(pBuf, lastVarEndIndirectOffset);
        return alignRoundUp(dataLen);
    }

    /**
     * Determines the number of bytes required to store a tuple without actually
     * marshalling it.
     *
     * @param tuple the tuple data
     *
     * @return byte count
     */
    public int getByteCount(FennelTupleData tuple)
    {
        if (isFixedWidth()) {
            return maxStorage;
        }

        int cb = firstVarOffset;
        int i;
        for (i = 0; i < varWidthAccessors.size(); ++i) {
            FennelTupleDatum datum =
                tuple.getDatum(
                    ((Integer) varWidthAccessors.get(i)).intValue());
            if (datum.isPresent()) {
                cb += datum.getLength();
            }
        }

        // round up for alignment padding
        return alignRoundUp(cb);
    }

    /**
     * Determines whether a buffer is big enough to fit marshalled tuple data.
     *
     * @param tuple the tuple to be marshalled
     * @param bufSize the size of the candidate buffer
     *
     * @return true if bufSize is big enough
     */
    public boolean isBufferSufficient(FennelTupleData tuple, int bufSize)
    {
        if (getMaxByteCount() <= bufSize) {
            return true;
        }
        return getByteCount(tuple) <= bufSize;
    }

    /**
     * Unmarshals the current tuple buffer, setting a tuple's values to
     * reference the contents.
     *
     * @param tuple the tuple which will be modified to reference the
     * unmarshalled values
     * @param iFirstDatum 0-based index of FennelTupleDatum at which to start
     * writing to tuple (defaults to first FennelTupleDatum); note that
     * unmarshalling always starts with the first attribute
     */
    public void unmarshal(FennelTupleData tuple, int iFirstDatum)
    {
        int n = tuple.getDatumCount() - iFirstDatum;
        if (n > attrAccessors.size()) {
            n = attrAccessors.size();
        }

        boolean sliced = false;
        ByteBuffer prevCurrent = currTupleBuf;

        // see if we're not at the beginning of the tuple buffer; if not
        // we have to slice it
        if (currTupleBuf.position() != 0) {
            while ((currTupleBuf.position() & 0x3) != 0) {
                currTupleBuf.position(currTupleBuf.position() + 1);
            }
            currTupleBuf = currTupleBuf.slice();
            if (setNativeOrder) {
                currTupleBuf.order(ByteOrder.nativeOrder());
            }
            sliced = true;
        }

        int i;
        for (i = 0; i < n; ++i) {
            FennelAttributeAccessor attr = getAttributeAccessor(i);
            if (!attr.isPresent(currTupleBuf)) {
                tuple.getDatum(iFirstDatum + i).reset();
            } else {
                attr.unmarshalValue(
                    this,
                    tuple.getDatum(iFirstDatum + i));
            }
        }
        currTupleBuf.position(getByteCount(tuple));
        if (sliced) {
            prevCurrent.position(
                prevCurrent.position() + currTupleBuf.position());
            currTupleBuf = prevCurrent;
        }
    }

    /**
     * Unmarshals the current tuple buffer, setting a tuple's values to
     * reference the contents. Assumes starting at the first FennelTupleDatum
     * (index zero).
     *
     * @param tuple the tuple which will be modified to reference the
     * unmarshalled values, starting with the first datum
     */
    public void unmarshal(FennelTupleData tuple)
    {
        unmarshal(tuple, 0);
    }

    /**
     * Gets an accessor for an individual attribute. This can be used to
     * unmarshall values individually.
     *
     * @param iAttribute 0-based index of the attribute within the tuple
     */
    public FennelAttributeAccessor getAttributeAccessor(int iAttribute)
    {
        return (FennelAttributeAccessor) attrAccessors.get(iAttribute);
    }

    /**
     * Marshalls a tuple's values into a buffer.
     *
     * @param tuple the tuple to be marshalled
     * @param tupleBuf the buffer into which to marshal (note that this
     * accessor's own current tuple buffer remains unchanged)
     */
    public void marshal(FennelTupleData tuple, ByteBuffer tupleBuf)
    {
        // support appending tuples to the end of an existing tuple buffer
        ByteBuffer workBuf;
        boolean sliced = false;
        if (tupleBuf.position() == 0) {
            tupleBuf.clear(); // why not?
            workBuf = tupleBuf;
        } else {
            while ((tupleBuf.position() & 0x3) != 0) {
                tupleBuf.put((byte) 0);
            }
            workBuf = tupleBuf.slice();
            sliced = true;
        }

        int iNextVarOffset = firstVarOffset;
        int pNextVarEndOffset = firstVarEndIndirectOffset;

        int i;

        // initialize any bitfields
        if (nBitFields > 0) {
            // FIXME - should be:
            //      int bfLen = (nBitFields+7)>>>3;
            // but that's tough to stomach. later, once it's proven
            int bfLen = (nBitFields + 7) / 8;
            for (i = 0; i < bfLen; i++) {
                workBuf.put(i + bitFieldOffset, (byte) 0);
            }
        }

        for (i = 0; i < tuple.getDatumCount(); i++) {
            int iAttr;
            if (marshalOrder != null) {
                iAttr = ((Integer) marshalOrder.get(i)).intValue();
            } else {
                iAttr = i;
            }
            FennelTupleDatum value = tuple.getDatum(iAttr);
            FennelAttributeAccessor accessor = getAccessor(iAttr);

            // set is-value-present for nullables
            if (accessor.nullBitNdx != Integer.MAX_VALUE) {
                accessor.setPresent(
                    workBuf,
                    value.isPresent());
            }

            if (value.isPresent()) {
                if (accessor.valueBitNdx == Integer.MAX_VALUE) {
                    int iOffset;
                    if (accessor.fixedOffset != Integer.MAX_VALUE) {
                        iOffset = accessor.fixedOffset;
                    } else {
                        iOffset = iNextVarOffset;
                    }
                    assert (value.getLength() <= accessor.capacity);
                    workBuf.position(iOffset);
                    accessor.marshalValueData(workBuf, value);
                } else {
                    accessor.marshalValueData(workBuf, value);
                }
            } else {
                // if you hit this assert, most likely the result produced
                // a null but type derivation in SqlValidator derived a
                // non nullable result type
                assert (accessor.nullBitNdx != Integer.MAX_VALUE);
            }
            if (accessor.endIndirectOffset != Integer.MAX_VALUE) {
                //                assert(pNextVarEndOffset ==
                //    referenceIndirectOffset(accessor.endIndirectOffset));
                if (value.isPresent()) {
                    iNextVarOffset += value.getLength();
                }

                // regardless of whether the value is null, we need to record
                // the end offset since it also marks the start of the next
                // non-null value
                workBuf.putShort(pNextVarEndOffset, (short) iNextVarOffset);
                pNextVarEndOffset += STOREDVALUEOFFSETSIZE;
            }
        }
        if (sliced) {
            tupleBuf.position(tupleBuf.position() + getByteCount(tuple));
        } else {
            tupleBuf.position(getByteCount(tuple));
        }
    }
}

// End FennelTupleAccessor.java
