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

#ifndef Fennel_IntrusiveDList_Included
#define Fennel_IntrusiveDList_Included

FENNEL_BEGIN_NAMESPACE

// TODO:  clean this up to follow the IntrusiveList pattern

/**
 * A link in an intrusive doubly-linked list.
 */
class FENNEL_COMMON_EXPORT IntrusiveDListNode
{
    IntrusiveDListNode *pPrev;
    IntrusiveDListNode *pNext;

public:
    explicit IntrusiveDListNode()
    {
        pPrev = pNext = NULL;
    }

    IntrusiveDListNode *getNext() const
    {
        return pNext;
    }

    IntrusiveDListNode *getPrev() const
    {
        return pPrev;
    }

    void detach()
    {
        if (pNext) {
            pNext->pPrev = pPrev;
        }
        if (pPrev) {
            pPrev->pNext = pNext;
        }
        pPrev = pNext = NULL;
    }

    void insertBefore(IntrusiveDListNode &newNext)
    {
        pNext = &newNext;
        pPrev = pNext->pPrev;
        pNext->pPrev = this;
        if (pPrev) {
            pPrev->pNext = this;
        }
    }

    void insertAfter(IntrusiveDListNode &newPrev)
    {
        pPrev = &newPrev;
        pNext = pPrev->pNext;
        pPrev->pNext = this;
        if (pNext) {
            pNext->pPrev = this;
        }
    }
};

/**
 * Iterator over an intrusive doubly-linked list.
 */
template <class T>
class IntrusiveDListIter
{
    T *curr;
public:

    explicit IntrusiveDListIter()
    {
        curr = NULL;
    }

    explicit IntrusiveDListIter(T *currInit)
    {
        curr = currInit;
    }

    void restart(T *currInit)
    {
        curr = currInit;
    }

    void operator ++ ()
    {
        curr = static_cast<T *>(curr->getNext());
    }

    void operator -- ()
    {
        curr = static_cast<T *>(curr->getPrev());
    }

    T *operator -> () const
    {
        return curr;
    }

    operator T * () const
    {
        return curr;
    }

    T & operator * () const
    {
        return *curr;
    }

    bool operator == (IntrusiveDListIter const &other) const
    {
        return curr == other.curr;
    }
};

/**
 * Iterator over two intrusive doubly-linked lists.  The first list is
 * walked followed by the second list.  The lists can only be walked over in
 * the forward direction.
 *
 * <p>
 * Elements in both lists are all of the same type.  A callback method must be
 * defined that determines what should be returned for each element in the
 * lists.
 */
template <class ElementT, class ReturnT>
class IntrusiveTwoDListIter
{
    /**
     * Pointer to the current list element
     */
    ElementT *curr;

    /**
     * Pointer to the start of the second list
     */
    ElementT *next;

    /**
     * True if the first list has been walked
     */
    bool processingNext;

protected:

    /**
     * Returns a pointer to the return element corresponding to the element
     * that the iterator is currently positioned at.
     *
     * @param element the current element
     *
     * @return pointer to the return element
     */
    virtual ReturnT *getReturnElement(ElementT *element) const = 0;

public:

    explicit IntrusiveTwoDListIter()
    {
        curr = NULL;
        next = NULL;
        processingNext = false;
    }

    explicit IntrusiveTwoDListIter(ElementT *list1, ElementT *list2)
    {
        if (list1 == NULL) {
            curr = list2;
            processingNext = true;
        } else {
            curr = list1;
            next = list2;
            processingNext = false;
        }
    }

    virtual ~IntrusiveTwoDListIter()
    {
    }

    void operator ++ ()
    {
        curr = static_cast<ElementT *>(curr->getNext());
        if (curr == NULL && !processingNext) {
            curr = next;
            processingNext = true;
        }
    }

    ReturnT *operator -> () const
    {
        return getReturnElement(curr);
    }

    operator ReturnT * () const
    {
        return getReturnElement(curr);
    }

    ReturnT & operator * () const
    {
        return *(getReturnElement(curr));
    }

    bool operator == (IntrusiveTwoDListIter const &other) const
    {
        return curr == other.curr;
    }
};

FENNEL_END_NAMESPACE

#endif

// End IntrusiveDList.h
