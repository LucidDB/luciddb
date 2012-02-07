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

#ifndef Fennel_TuplePrinter_Included
#define Fennel_TuplePrinter_Included

#include "fennel/common/DataVisitor.h"

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

class TupleDescriptor;
class TupleData;

class FENNEL_TUPLE_EXPORT TuplePrinter
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
    virtual void visitChars(char const *, TupleStorageByteLength nChars);
    virtual void visitUnicodeChars(Ucs2ConstBuffer, uint nChars);
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
