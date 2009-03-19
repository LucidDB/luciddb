/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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

#ifndef Fennel_CompoundId_Included
#define Fennel_CompoundId_Included

FENNEL_BEGIN_NAMESPACE

// NOTE:  read the comment on struct StoredNode before
// modifying the encoding below

/**
 * CompoundId is a collection of static methods for manipulating PageIds,
 * BlockIds, and SegByteIds.  These ID's are encoded into 64-bit
 * integers representing logical or physical storage locations as follows:
 *
 *<p>
 *<pre>
 * MSB  [ 12-bit DeviceId : 20-bit byte offset: 32-bit BlockNum ]  LSB
 *</pre>
 *
 *<p>
 *
 * where DeviceId is the ID of some cached device, BlockNum is the block number
 * of some block within that device (actual offset is determined by the device
 * type and block size), and the byte offset is a 0-based byte location within
 * that block.
 *
 *<p>
 *
 * The bit arrangement above allows for page sizes up to 1M.  Combined with a
 * 32-bit BlockNum, this allows a device size up to 4 petabytes.  It also
 * optimizes for translation between PageId and BlockId, rather than for byte
 * offset access.  The main justification is that PageId/BlockId translation
 * has to occur for every page accessed, and in many cases the byte offset may
 * not even be used.  However, this means that SegByteId should only be used as
 * a means of compact persistent representation; any time significant byte
 * offset arithmetic has to be done, the byte offset should be manipulated as a
 * separate uint.
 *
 *<p>
 *
 * TODO:  autoconf support for parameterizing the CompoundId representation,
 * since one size may not fit all.  Also, could use architecture-dependent code
 * for speeding up the accessors.
 *
 *<p>
 *
 * For a PageId or BlockId, the byte offset portion is always 0 (except for
 * NULL_PAGE_ID and NULL_BLOCK_ID).
 */
class CompoundId
{
     // masks for extracting portions of a PageId, BlockId, or SegByteId
    static const uint64_t DEVICE_ID_MASK =   0xFFF0000000000000ULL;
    static const uint64_t BYTE_OFFSET_MASK = 0x000FFFFF00000000ULL;
    static const uint64_t BLOCK_NUM_MASK =   0x00000000FFFFFFFFULL;

    /**
     * Number of bits to right-shift a masked PageId to extract the DeviceId.
     */
    static const uint DEVICE_ID_SHIFT = 52;

    /**
     * Number of bits to right-shift a masked PageId to extract the BlockNum.
     */
    static const uint BYTE_OFFSET_SHIFT = 32;

public:
    /**
     * Maximum number of devices, based on mask sizes.
     */
    static const uint MAX_DEVICES = 0x1000;

    /**
     * Maximum byte offset on a page.
     */
    static const uint MAX_BYTE_OFFSET = 0xFFFFF;

    /**
     * Extracts the BlockNum from a PageId or BlockId.
     *
     * @param pageId the PageId or BlockId to access
     *
     * @return the extracted BlockNum
     */
    template <class PageOrBlockId>
    static BlockNum getBlockNum(PageOrBlockId pageId)
    {
        return (opaqueToInt(pageId) & BLOCK_NUM_MASK);
    }

    /**
     * Sets just the BlockNum of a PageId or BlockId.
     *
     * @param pageId the PageId or BlockId to modify
     *
     * @param blockNum the new BlockNum to set
     */
    template <class PageOrBlockId>
    static void setBlockNum(PageOrBlockId &pageId,BlockNum blockNum)
    {
        assert(blockNum == (blockNum & BLOCK_NUM_MASK));
        pageId = PageOrBlockId(
            (opaqueToInt(pageId) & DEVICE_ID_MASK) | blockNum);
    }

    /**
     * Increments the BlockNum of a PageId or BlockId.
     *
     * @param pageId the PageId or BlockId to modify
     */
    template <class PageOrBlockId>
    static void incBlockNum(PageOrBlockId &pageId)
    {
        setBlockNum(pageId,getBlockNum(pageId) + 1);
    }

    /**
     * Decrements the BlockNum of a PageId or BlockId.
     *
     * @param pageId the PageId or BlockId to modify
     */
    template <class PageOrBlockId>
    static void decBlockNum(PageOrBlockId &pageId)
    {
        setBlockNum(pageId,getBlockNum(pageId) - 1);
    }

    /**
     * Extracts the DeviceId from a PageId or BlockId.
     *
     * @param pageId the PageId or BlockId to access
     *
     * @return the extracted DeviceId
     */
    template <class PageOrBlockId>
    static DeviceId getDeviceId(PageOrBlockId pageId)
    {
        return DeviceId(
            (opaqueToInt(pageId) & DEVICE_ID_MASK) >> DEVICE_ID_SHIFT);
    }

    /**
     * Sets just the DeviceId of a PageId or BlockId.
     *
     * @param pageId the PageId or BlockId to modify
     *
     * @param deviceId the new DeviceId to set
     */
    template <class PageOrBlockId>
    static void setDeviceId(PageOrBlockId &pageId,DeviceId deviceId)
    {
        assert(opaqueToInt(deviceId) < MAX_DEVICES);
        pageId = PageOrBlockId(
            (uint64_t(opaqueToInt(deviceId)) << DEVICE_ID_SHIFT)
            | getBlockNum(pageId));
    }

    /**
     * Extracts the PageId component of a SegByteId.
     *
     * @param segByteId the SegByteId to access
     *
     * @return the extracted PageId
     */
    static PageId getPageId(SegByteId segByteId)
    {
        return PageId(opaqueToInt(segByteId) & ~BYTE_OFFSET_MASK);
    }

    /**
     * Sets the PageId component of a SegByteId.
     *
     * @param segByteId the SegByteId to modify
     *
     * @param pageId the new PageId to set
     */
    static void setPageId(SegByteId &segByteId,PageId pageId)
    {
        segByteId = SegByteId(
            opaqueToInt(pageId)
            | (opaqueToInt(segByteId) & BYTE_OFFSET_MASK));
    }

    /**
     * Extracts the byte offset component of a SegByteId.
     *
     * @param segByteId the SegByteId to access
     *
     * @return the extracted byte offset
     */
    static uint getByteOffset(SegByteId segByteId)
    {
        return (opaqueToInt(segByteId) & BYTE_OFFSET_MASK) >> BYTE_OFFSET_SHIFT;
    }

    /**
     * Sets the byte offset component of a SegByteId.
     *
     * @param segByteId the SegByteId to modify
     *
     * @param offset the new byte offset to set
     */
    static void setByteOffset(SegByteId &segByteId,uint offset)
    {
        assert(offset == (offset & (BYTE_OFFSET_MASK >> BYTE_OFFSET_SHIFT)));
        segByteId = SegByteId(
            opaqueToInt(getPageId(segByteId))
            | (SegByteIdPrimitive(offset) << BYTE_OFFSET_SHIFT));
    }

    /**
     * Compares two PageIds.
     *
     * @param p1 first PageId to compare
     *
     * @param p2 second PageId to compare
     *
     * @return memcmp convention (negative if p1 is less than p2; zero if
     * equal; positive if greater)
     */
    static int comparePageIds(PageId p1,PageId p2)
    {
        return (p1 > p2) ? 1
            : ((p1 < p2) ? -1 : 0);
    }

    /**
     * Compares two SegByteIds.
     *
     * @param t1 first SegByteId to compare
     *
     * @param t2 second SegByteId to compare
     *
     * @return memcmp convention (negative if t1 is less than t2; zero if
     * equal; positive if greater)
     */
    static int compareSegByteIds(SegByteId t1,SegByteId t2)
    {
        return (t1 > t2) ? 1
            : ((t1 < t2) ? -1 : 0);
    }

    /**
     * @return the maximum number of devices permitted by the page ID
     * encoding
     */
    static uint getMaxDeviceCount()
    {
        return MAX_DEVICES;
    }
};

FENNEL_END_NAMESPACE

#endif

// End CompoundId.h
