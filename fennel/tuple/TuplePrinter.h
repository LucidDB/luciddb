/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
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

#ifndef Fennel_TuplePrinter_Included
#define Fennel_TuplePrinter_Included

#include "fennel/common/DataVisitor.h"

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

class TupleDescriptor;
class TupleData;

class TuplePrinter
    : public boost::noncopyable,
    private DataVisitor
{
    std::ostream *pStream;
    uint iValue;

    void preVisitValue();
    void postVisitValue();
    
    // implement DataVisitor
    virtual void preVisitDocument(std::string);
    virtual void postVisitDocument();
    virtual void preVisitTable(std::string title);
    virtual void postVisitTable();
    virtual void preVisitRow();
    virtual void postVisitRow();
    virtual void visitAttribute(std::string);
    virtual void visitString(std::string);
    virtual void visitChars(char const *, TupleStorageByteLength iChars);
    virtual void visitUnsignedInt(uint64_t);
    virtual void visitSignedInt(int64_t);
    virtual void visitDouble(double);
    virtual void visitFloat(float);
    virtual void visitBoolean(bool);
    virtual void visitPageId(PageId);
    virtual void visitPageOwnerId(PageOwnerId);
    virtual void visitSegByteId(SegByteId);
    virtual void visitFormatted(char const *);
    virtual void visitBytes(void const *v, TupleStorageByteLength iBytes);
    
public:
    explicit TuplePrinter();

    void print(
        std::ostream &stream,
        TupleDescriptor const &tupleDesc,
        TupleData const &tupleData);
};

FENNEL_END_NAMESPACE

#endif

// End TuplePrinter.h
