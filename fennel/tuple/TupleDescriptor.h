/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2003-2007 Disruptive Tech
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
struct TupleAttributeDescriptor
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
class TupleProjection : public std::vector<uint>
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
class TupleDescriptor : public std::vector<TupleAttributeDescriptor>
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

std::ostream &operator<<(std::ostream &str,TupleDescriptor const &);

std::ostream &operator<<(std::ostream &str,TupleAttributeDescriptor const &);

FENNEL_END_NAMESPACE

#endif

// End TupleDescriptor.h
