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
