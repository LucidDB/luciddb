/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifndef Fennel_IntrusiveList_Included
#define Fennel_IntrusiveList_Included

#include "fennel/common/RawIntrusiveList.h"

FENNEL_BEGIN_NAMESPACE

/**
 * IntrusiveList is a singly-linked list which requires its elements to derive
 * from IntrusiveListNode (endowing them with the required forward links).
 * This eliminates the need for dynamic allocation, which is important for some
 * performance-critical uses.  It is possible for elements to be members of
 * multiple IntrusiveLists simultaneously; however, this requires that they
 * derive from IntrusiveListNode multiple times (one for each link).  Template
 * parameter T is the datatype of the element stored; template parameter
 * DerivedListNode is a class derived from IntrusiveListNode where T inherits
 * from DerivedListNode.  If T participates in only one IntrusiveList, then it
 * can derive from IntrusiveListNode directly, and the default value for
 * DerivedListNode can be used.  Otherwise, one derivation of IntrusiveListNode
 * per link should be defined (with an appropriate name to spell out its
 * purpose) and specified as the DerivedListNode parameter.
 *
 *<p>
 *
 * Although method names have been chosen to match STL conventions,
 * IntrusiveList is not a true STL container (likewise, IntrusiveListIter is
 * not a true STL iterator).
 */
template <class T,class DerivedListNode = IntrusiveListNode>
class IntrusiveList : public RawIntrusiveList
{
    typedef RawIntrusiveList super;
    
public:
    /**
     * Adds an element to the front of the list.
     *
     * @param element the element to add
     */
    void push_front(T &element)
    {
        super::push_front(static_cast<DerivedListNode &>(element));
    }
    
    /**
     * Adds an element to the back of the list.
     *
     * @param element the element to add
     */
    void push_back(T &element)
    {
        super::push_back(static_cast<DerivedListNode &>(element));
    }

    /**
     * @return a reference to the element at the front of the list
     */
    T &front() const
    {
        return static_cast<T &>(
            static_cast<DerivedListNode &>(super::front()));
    }
    
    /**
     * @return a reference to the element at the back of the list
     */
    T &back() const
    {
        return static_cast<T &>(
            static_cast<DerivedListNode &>(super::back()));
    }
    
    /**
     * Finds and removes a specified element by address (not equality).
     *
     * @param element the element to be removed
     *
     * @return true if found
     */
    bool remove(T &element)
    {
        return super::remove(
            static_cast<DerivedListNode &>(element));
    }
};

/**
 * IntrusiveListIter is the companion iterator for InstrusiveList.
 */
template <class T,class DerivedListNode = IntrusiveListNode>
class IntrusiveListIter : public RawIntrusiveListIter
{
    typedef RawIntrusiveListIter super;
    
public:
    /**
     * Constructs a singular iterator.
     */
    explicit IntrusiveListIter() 
    {
    }
    
    /**
     * Constructs an iterator positioned at the front of a given list.
     *
     * @param list the list to access
     */
    explicit IntrusiveListIter(
        IntrusiveList<T,DerivedListNode> const &list)
        : super(list)
    {
    }

    /**
     * @return pointer to current element
     */
    T *operator -> () const
    {
        return static_cast<T *>(
            static_cast<DerivedListNode *>(getCurrent()));
    }
    
    /**
     * @return pointer to current element
     */
    operator T * () const
    {
        return static_cast<T *>(
            static_cast<DerivedListNode *>(getCurrent()));
    }
    
    /**
     * @return reference to current element
     */
    T & operator * () const
    {
        return *static_cast<T *>(
            static_cast<DerivedListNode *>(getCurrent()));
    }

    /**
     * Repositions to the front of a given list.
     *
     * @param list the list to access
     */
    void repositionToFront(
        IntrusiveList<T,DerivedListNode> const &list)
    {
        super::repositionToFront(list);
    }
};                                        

/**
 * IntrusiveListMutator is the companion mutator for InstrusiveList.  It allows
 * the list to be modified during the course of iteration.
 */
template <class T,class DerivedListNode = IntrusiveListNode>
class IntrusiveListMutator : public RawIntrusiveListMutator
{
    typedef RawIntrusiveListMutator super;
    
public:                                
    /**
     * Constructs a singular mutator.
     */
    explicit IntrusiveListMutator() 
    {
    }
    
    /**
     * Constructs a mutator positioned at the front of a given list.
     *
     * @param list the list to access
     */
    explicit IntrusiveListMutator(
        IntrusiveList<T,DerivedListNode> &list)
        : super(list)
    {
    }
    
    /**
     * @return pointer to current element
     */
    T *operator -> () const
    {
        return static_cast<T *>(
            static_cast<DerivedListNode *>(getCurrent()));
    }
    
    /**
     * @return reference to current element
     */
    operator T * () const
    {
        return static_cast<T *>(
            static_cast<DerivedListNode *>(getCurrent()));
    }
    
    /**
     * @return reference to current element
     */
    T & operator * () const
    {
        return *static_cast<T *>(
            static_cast<DerivedListNode *>(getCurrent()));
    }

    /**
     * Removes current element from list.  This mutator will advance to the
     * original successor on next increment.
     *
     * @return element removed
     */
    T *detach()
    {
        return static_cast<T *>(
            static_cast<DerivedListNode *>(super::detach()));
    }
    
    /**
     * Repositions this mutator to the front of a given list.
     *
     * @param list the list to access
     */
    void repositionToFront(
        IntrusiveList<T,DerivedListNode> &list)
    {
        super::repositionToFront(list);
    }
    
    /**
     * Repositions this mutator to the front of the current list.
     */
    void repositionToFront()
    {
        super::repositionToFront();
    }
};

FENNEL_END_NAMESPACE

#endif

// End IntrusiveList.h
