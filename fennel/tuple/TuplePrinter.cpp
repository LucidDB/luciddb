/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2003-2009 SQLstream, Inc.
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/tuple/TupleDescriptor.h"

#include <boost/io/ios_state.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

TuplePrinter::TuplePrinter()
{
    pStream = NULL;
}

void TuplePrinter::print(
    std::ostream &stream,
    TupleDescriptor const &tupleDesc,
    TupleData const &tupleData)
{
    boost::io::ios_all_saver streamStateSaver(stream);
    pStream = &stream;
    iValue = 0;
    (*pStream) << "[ ";
    tupleDesc.visit(tupleData, *this, false);
    (*pStream) << " ]";
    pStream = NULL;
}

void TuplePrinter::preVisitValue()
{
    if (iValue) {
        (*pStream) << ", ";
    }
}

void TuplePrinter::postVisitValue()
{
    ++iValue;
}

void TuplePrinter::visitString(std::string s)
{
    preVisitValue();
    // TODO:  escaping
    (*pStream) << "'" << s << "'";
    postVisitValue();
}

void TuplePrinter::visitChars(char const *c, TupleStorageByteLength n)
{
    std::string s(c, n);
    visitString(s);
}

void TuplePrinter::visitUnicodeChars(Ucs2ConstBuffer c, uint n)
{
    // TODO jvs 13-Jan-2009:  something prettier
    visitBytes(c, n*2);
}

void TuplePrinter::visitUnsignedInt(uint64_t i)
{
    preVisitValue();
    (*pStream) << i;
    postVisitValue();
}

void TuplePrinter::visitSignedInt(int64_t i)
{
    preVisitValue();
    // FIXME:  this comes out as garbage for the smallest negative value; not
    // sure why
    (*pStream) << i;
    postVisitValue();
}

void TuplePrinter::visitDouble(double d)
{
    preVisitValue();
    (*pStream) << d;
    postVisitValue();
}

void TuplePrinter::visitFloat(float f)
{
    preVisitValue();
    (*pStream) << f;
    postVisitValue();
}

void TuplePrinter::visitBoolean(bool b)
{
    preVisitValue();
    if (b) {
        (*pStream) << "true";
    } else {
        (*pStream) << "false";
    }
    postVisitValue();
}

void TuplePrinter::visitBytes(void const *v, TupleStorageByteLength iBytes)
{
    preVisitValue();
    if (!v) {
        (*pStream) << "NULL";
    } else {
        hexDump(*pStream,v,iBytes);
    }
    postVisitValue();
}

void TuplePrinter::preVisitDocument(std::string)
{
    // unused
}

void TuplePrinter::postVisitDocument()
{
    // unused
}

void TuplePrinter::preVisitTable(std::string)
{
    // unused
}

void TuplePrinter::postVisitTable()
{
    // unused
}

void TuplePrinter::preVisitRow()
{
    // unused
}

void TuplePrinter::postVisitRow()
{
    // unused
}

void TuplePrinter::visitAttribute(std::string)
{
    // unused
}

void TuplePrinter::visitPageId(PageId)
{
    // unused
}

void TuplePrinter::visitPageOwnerId(PageOwnerId)
{
    // unused
}

void TuplePrinter::visitSegByteId(SegByteId)
{
    // unused
}

void TuplePrinter::visitFormatted(char const *)
{
    // unused
}

FENNEL_END_CPPFILE("$Id$");

// End TuplePrinter.cpp
