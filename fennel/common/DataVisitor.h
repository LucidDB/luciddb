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

#ifndef Fennel_DataVisitor_Included
#define Fennel_DataVisitor_Included

FENNEL_BEGIN_NAMESPACE

/**
 * Visitor interface for dump/check/repair functions.
 *
 *<p>
 *
 * TODO:  doc
 */
class DataVisitor
{
public:
    virtual ~DataVisitor();
    virtual void preVisitDocument(std::string) = 0;
    virtual void postVisitDocument() = 0;
    virtual void preVisitTable(std::string title) = 0;
    virtual void postVisitTable() = 0;
    virtual void preVisitRow() = 0;
    virtual void postVisitRow() = 0;
    virtual void visitAttribute(std::string) = 0;
    virtual void visitString(std::string) = 0;
    virtual void visitChars(char const *, uint iChars) = 0;
    virtual void visitUnsignedInt(uint64_t) = 0;
    virtual void visitSignedInt(int64_t) = 0;
    virtual void visitDouble(double) = 0;
    virtual void visitFloat(float) = 0;
    virtual void visitBoolean(bool) = 0;
    virtual void visitPageId(PageId) = 0;
    virtual void visitPageOwnerId(PageOwnerId) = 0;
    virtual void visitSegByteId(SegByteId) = 0;
    virtual void visitFormatted(char const *) = 0;
    virtual void visitBytes(void const *v,uint iBytes) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End DataVisitor.h
