/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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
};

/**
 * A TupleDescriptor specifies a vector of stored attributes, as explained in
 * the <a href="structTupleDesign.html#TupleDescriptor">design docs</a>.
 */
class TupleDescriptor : public std::vector<TupleAttributeDescriptor>
{
public:
    void projectFrom(
        TupleDescriptor const &tupleDescriptor,
        TupleProjection const &tupleProjection);

    int compareTuples(
        TupleData const &tuple1,
        TupleData const &tuple2) const;

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

};

std::ostream &operator<<(std::ostream &str,TupleDescriptor const &);

std::ostream &operator<<(std::ostream &str,TupleAttributeDescriptor const &);

FENNEL_END_NAMESPACE

#endif

// End TupleDescriptor.h
