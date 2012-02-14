/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
