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

#ifndef Fennel_RawIntrusiveList_Included
#define Fennel_RawIntrusiveList_Included

FENNEL_BEGIN_NAMESPACE

class RawIntrusiveList;
class RawIntrusiveListIter;
class RawIntrusiveListMutator;

/**
 * RawIntrusiveList is the type-unsafe implementation for the type-safe
 * IntrusiveList template.  See InstrusiveList for details.
 */
class FENNEL_COMMON_EXPORT RawIntrusiveList
{
    friend class RawIntrusiveListIter;
    friend class RawIntrusiveListMutator;

protected:
    /**
     * Number of nodes in this list.
     */
    uint nNodes;

    /**
     * First node in this list.
     */
    IntrusiveListNode *pFront;

    /**
     * Last node in this list.
     */
    IntrusiveListNode *pBack;

    explicit RawIntrusiveList()
    {
        pFront = NULL;
        pBack = NULL;
        nNodes = 0;
    }

    IntrusiveListNode &front() const
    {
        return *pFront;
    }

    IntrusiveListNode &back() const
    {
        return *pBack;
    }

    void push_front(IntrusiveListNode &t);

    void push_back(IntrusiveListNode &t);

    bool remove(IntrusiveListNode &);

public:
    /**
     * @return length of this list
     */
    uint size() const
    {
        return nNodes;
    }

    /**
     * @return true iff size() is zero
     */
    bool empty() const
    {
        return nNodes ? false : true;
    }

    /**
     * Truncates this list to zero nodes.
     *
     * @param debugClear when true and a DEBUG build is in effect, pNext links
     * of nodes are nulled out
     */
    void clear(bool debugClear = true);
};

/**
 * RawIntrusiveListIter is the type-unsafe implementation for the type-safe
 * IntrusiveListIter template.  See InstrusiveListIter for details.
 */
class FENNEL_COMMON_EXPORT RawIntrusiveListIter {
protected:
    IntrusiveListNode *pCurr;

    explicit RawIntrusiveListIter()
        : pCurr(NULL)
    {
    }

    explicit RawIntrusiveListIter(RawIntrusiveList const &l)
        : pCurr(l.pFront)
    {
    }

    IntrusiveListNode *getCurrent() const
    {
        return pCurr;
    }

    void repositionToFront(RawIntrusiveList const &l)
    {
        pCurr = l.pFront;
    }

public:
    /**
     * Advances iterator position to next node.
     */
    void operator ++ ()
    {
        if (pCurr) {
            pCurr = pCurr->pNext;
        }
    }
};

/**
 * RawIntrusiveListMutator is the type-unsafe implementation for the type-safe
 * IntrusiveListMutator template.  See InstrusiveListMutator for details.
 */
class FENNEL_COMMON_EXPORT RawIntrusiveListMutator
{
    friend class RawIntrusiveList;

protected:
    IntrusiveListNode *pCurr,*pPrev;
    RawIntrusiveList *pList;
    bool bJustDeleted;

    explicit RawIntrusiveListMutator()
    {
        pList = NULL;
        pCurr = pPrev = NULL;
        bJustDeleted = 0;
    }

    explicit RawIntrusiveListMutator(RawIntrusiveList &l)
        : pCurr(l.pFront), pPrev(NULL), pList(&l)
    {
        bJustDeleted = 0;
    }

    IntrusiveListNode *getCurrent() const
    {
        return pCurr;
    }

    void repositionToFront(RawIntrusiveList &l)
    {
        pList = &l;
        bJustDeleted = 0;
        pCurr = pList->pFront;
        pPrev = NULL;
    }

    IntrusiveListNode *detach();

public:
    /**
     * Advances iterator position to next node.
     */
    void operator ++ ()
    {
        if (bJustDeleted) {
            bJustDeleted = 0;
        } else {
            pPrev = pCurr;
            if (pCurr) {
                pCurr = pCurr->pNext;
            }
        }
    }

    /**
     * Moves the current node to the front of the list.  This iterator will
     * advance to the original successor on next increment.
     */
    void promoteCurrToFront();

    /**
     * Moves the current node to the back of the list.  This iterator will
     * advance to the original successor on next increment.
     */
    void demoteCurrToBack();

    /**
     * Moves the front node of the list to just before the current node.
     * Iterator position is unchanged.
     */
    void demoteFrontBeforeCurr();

    /**
     * Moves iterator position to front of list.
     */
    void repositionToFront()
    {
        bJustDeleted = 0;
        pCurr = pList->pFront;
        pPrev = NULL;
    }
};

FENNEL_END_NAMESPACE

#endif

// End RawIntrusiveList.h
