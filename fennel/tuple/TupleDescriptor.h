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

#ifndef Fennel_TupleDescriptor_Included
#define Fennel_TupleDescriptor_Included

#include <vector>

FENNEL_BEGIN_NAMESPACE

class StoredTypeDescriptor;
class StoredTypeDescriptorFactory;
class TupleData;
class ByteOutputStream;
class ByteInputStream;
class DataVisitor;

/**
 * A TupleAttributeDescriptor is a component of a TupleDescriptor, as explained
 * in the <a href="structTupleDesign.html#TupleDescriptor">design docs</a>.
 */
struct FENNEL_TUPLE_EXPORT TupleAttributeDescriptor
{
    StoredTypeDescriptor const *pTypeDescriptor;
    bool isNullable;
    TupleStorageByteLength cbStorage;

    explicit TupleAttributeDescriptor();

    explicit TupleAttributeDescriptor(
        StoredTypeDescriptor const &typeDescriptor,
        bool isNullable = false,
        TupleStorageByteLength cbStorage = 0);

    bool operator == (TupleAttributeDescriptor const &other) const;
};

/**
 * A TupleProjection specifies a projection of a tuple, as explained in
 * the <a href="structTupleDesign.html#TupleProjection">design docs</a>.
 */
class FENNEL_TUPLE_EXPORT TupleProjection
    : public VectorOfUint
{
public:
    void writePersistent(
        ByteOutputStream &) const;

    void readPersistent(
        ByteInputStream &);

    void projectFrom(
        TupleProjection const &sourceProjection,
        TupleProjection const &tupleProjection);
};

/**
 * A TupleDescriptor specifies a vector of stored attributes, as explained in
 * the <a href="structTupleDesign.html#TupleDescriptor">design docs</a>.
 *
 *<p>
 *
 * The compareTuples[Key] methods return the standard zero, negative,
 * or positive to indicate EQ, LT, GT.  However, rather than returning
 * -1 or 1 for LT/GT, they return the 1-based ordinal of the first
 * non-equal column (negated if LT).  This allows a caller to
 * implement ORDER BY DESC without having to pass in ASC/DESC information.
 */
class FENNEL_TUPLE_EXPORT TupleDescriptor
    : public std::vector<TupleAttributeDescriptor>
{
public:
    void projectFrom(
        TupleDescriptor const &tupleDescriptor,
        TupleProjection const &tupleProjection);

    // Utility function to compare the prefixes of two tuples up to keyCount.
    int compareTuplesKey(
        TupleData const &tuple1,
        TupleData const &tuple2,
        uint keyCount) const;

    int compareTuples(
        TupleData const &tuple1,
        TupleData const &tuple2) const;

    int compareTuples(
        TupleData const &tuple1, TupleProjection const &proj1,
        TupleData const &tuple2, TupleProjection const &proj2,
        bool *containsNullKey = NULL) const;

    void writePersistent(
        ByteOutputStream &) const;

    void readPersistent(
        ByteInputStream &,
        StoredTypeDescriptorFactory const &);

    void visit(
        TupleData const &tuple,
        DataVisitor &dataVisitor,
        bool visitLengths) const;

    bool containsNullable() const;

    /** Performs a comparison only of type & size, not nullability */
    bool storageEqual(
        TupleDescriptor const &other) const;
    TupleStorageByteLength getMaxByteCount() const;
};

FENNEL_TUPLE_EXPORT std::ostream & operator<< (
    std::ostream &str,TupleDescriptor const &);

FENNEL_TUPLE_EXPORT std::ostream & operator<< (
    std::ostream &str,TupleAttributeDescriptor const &);

FENNEL_END_NAMESPACE

#endif

// End TupleDescriptor.h
