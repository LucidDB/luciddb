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

#ifndef Fennel_BTreeAccessBase_Included
#define Fennel_BTreeAccessBase_Included

#include "fennel/btree/BTreeDescriptor.h"
#include "fennel/tuple/TupleProjectionAccessor.h"
#include "fennel/tuple/AttributeAccessor.h"

#include <boost/utility.hpp>
#include <boost/scoped_ptr.hpp>

FENNEL_BEGIN_NAMESPACE

class AttributeAccessor;
class BTreeNode;
class BTreeNodeAccessor;

/**
 * BTreeAccessBase is a base for classes which access BTree contents.  It
 * declares some of the necessary tuple accessors, helper methods, etc.
 */
class BTreeAccessBase : public boost::noncopyable
{
protected:
    /**
     * Descriptor for tree being accessed.
     */
    BTreeDescriptor treeDescriptor;
    
    /**
     * Descriptor for pure keys (common across leaf and non-leaf tuples).
     */
    TupleDescriptor keyDescriptor;

    /**
     * Accessor for the attribute of non-leaf tuples which stores the child
     * PageId.
     */
    AttributeAccessor const *pChildAccessor;

    /**
     * Accessor for keys of tuples stored in leaves.
     */
    TupleProjectionAccessor leafKeyAccessor;

    /**
     * Accessor for non-leaf nodes.
     */
    boost::scoped_ptr<BTreeNodeAccessor> pNonLeafNodeAccessor;
    
    /**
     * Accessor for leaf nodes.
     */
    boost::scoped_ptr<BTreeNodeAccessor> pLeafNodeAccessor;

    // ----------------------------------------------------------------------
    // protected inlines below are defined in BTreeAccessBaseImpl.h
    // ----------------------------------------------------------------------

    /**
     * Get the node accessor for a leaf node.  This method asserts that the
     * node really is a leaf.
     *
     * @param node leaf node to access
     *
     * @return node accessor
     */
    inline BTreeNodeAccessor &getLeafNodeAccessor(BTreeNode const &node);
    
    /**
     * Get the node accessor for a non-leaf node.  This method asserts that the
     * node really is a non-leaf.
     *
     * @param node non-leaf node to access
     *
     * @return node accessor
     */
    inline BTreeNodeAccessor &getNonLeafNodeAccessor(BTreeNode const &node);

    /**
     * Get the node accessor for any node.  This method uses the node
     * height to determine whether it's a leaf or not.  If you already know
     * this from the context, use getLeafNodeAccessor or
     * getNonLeafNodeAccessor instead.
     *
     * @param node node to access
     *
     * @return node accessor
     */
    inline BTreeNodeAccessor &getNodeAccessor(BTreeNode const &node);

    /**
     * Get the child PageId corresponding to the current key in a non-leaf
     * node.  This method assumes that accessTuple has already been called on
     * pNonLeafNodeAccessor (but can't assert this), so use with caution.
     *
     * @return child PageId
     */
    inline PageId getChildForCurrent();
    
    /**
     * Access a non-leaf tuple and get its child PageId.
     *
     * @param node non-leaf node to access
     *
     * @param iChild 0-based index of tuple to access
     *
     * @return child PageId of accessed tuple
     */
    inline PageId getChild(BTreeNode const &node,uint iChild);

    /**
     * Get the right sibling of a node.  This should only be used when the node
     * is not already locked (e.g. during prefetch).  When the node is
     * already locked, access its rightSibling field instead.
     *
     * @param pageId PageId of node whose sibling is to be found
     *
     * @return PageId of right sibling
     */
    inline PageId getRightSibling(PageId pageId);

    /**
     * Set the right sibling of a node.
     *
     * @param leftNode node whose right sibling is to be set
     *
     * @param leftPageId PageId corresponding to leftNode
     *
     * @param rightPageId PageId of new right sibling
     */
    inline void setRightSibling(
        BTreeNode &leftNode,PageId leftPageId,PageId rightPageId);
    
    /**
     * Get the first child of a non-leaf node.
     *
     * @param pageId PageId of non-leaf node
     *
     * @return PageId of node's first child
     */
    PageId getFirstChild(PageId pageId);

    explicit BTreeAccessBase(BTreeDescriptor const &descriptor);
    virtual ~BTreeAccessBase();

public:

    /**
     * @return the segment storing the BTree being accessed
     */
    inline SharedSegment getSegment() const;

    /**
     * @return the CacheAccessor used to access the BTree's pages
     */
    inline SharedCacheAccessor getCacheAccessor() const;
    
    /**
     * @return the BTree's root PageId
     */
    inline PageId getRootPageId() const;

    /**
     * Update the BTree's root PageId
     */
    void setRootPageId(PageId rootPageId);

    /**
     * @return SegmentId of segment storing the BTree
     */
    inline SegmentId getSegmentId() const;

    /**
     * @return PageOwnerId used to mark pages of the BTree
     */
    inline PageOwnerId getPageOwnerId() const;

    /**
     * @return TupleDescriptor for tuples stored by this BTree
     */
    inline TupleDescriptor const &getTupleDescriptor() const;
    
    /**
     * @return TupleDescriptor for keys indexed by this BTree
     */
    inline TupleDescriptor const &getKeyDescriptor() const;

    /**
     * @return TupleProjection from getTupleDescriptor() to getKeyDescriptor()
     */
    inline TupleProjection const &getKeyProjection() const;
};

inline SharedSegment BTreeAccessBase::getSegment() const
{
    return treeDescriptor.segmentAccessor.pSegment;
}

inline SharedCacheAccessor BTreeAccessBase::getCacheAccessor() const
{
    return treeDescriptor.segmentAccessor.pCacheAccessor;
}
    
inline PageId BTreeAccessBase::getRootPageId() const
{
    return treeDescriptor.rootPageId;
}

inline SegmentId BTreeAccessBase::getSegmentId() const
{
    return treeDescriptor.segmentId;
}

inline PageOwnerId BTreeAccessBase::getPageOwnerId() const
{
    return treeDescriptor.pageOwnerId;
}

inline TupleDescriptor const &BTreeAccessBase::getTupleDescriptor() const
{
    return treeDescriptor.tupleDescriptor;
}
    
inline TupleDescriptor const &BTreeAccessBase::getKeyDescriptor() const
{
    return keyDescriptor;
}

inline TupleProjection const &BTreeAccessBase::getKeyProjection() const
{
    return treeDescriptor.keyProjection;
}

FENNEL_END_NAMESPACE

#endif

// End BTreeAccessBase.h
