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
class FENNEL_COMMON_EXPORT DataVisitor
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
    virtual void visitChars(char const *, TupleStorageByteLength nChars) = 0;
    virtual void visitUnicodeChars(Ucs2ConstBuffer, uint nChars) = 0;
    virtual void visitUnsignedInt(uint64_t) = 0;
    virtual void visitSignedInt(int64_t) = 0;
    virtual void visitDouble(double) = 0;
    virtual void visitFloat(float) = 0;
    virtual void visitBoolean(bool) = 0;
    virtual void visitPageId(PageId) = 0;
    virtual void visitPageOwnerId(PageOwnerId) = 0;
    virtual void visitSegByteId(SegByteId) = 0;
    virtual void visitFormatted(char const *) = 0;
    virtual void visitBytes(void const *v,TupleStorageByteLength iBytes) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End DataVisitor.h
