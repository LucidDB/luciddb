/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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
class IntrusiveDListNode
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

FENNEL_END_NAMESPACE

#endif

// End IntrusiveDList.h
