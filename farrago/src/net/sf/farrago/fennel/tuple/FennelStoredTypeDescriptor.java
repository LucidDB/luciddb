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
package net.sf.farrago.fennel.tuple;

/**
 * FennelStoredTypeDescriptor provides an abstraction to describe a type of data
 * element supported by the tuple library.
 *
 * <p>StoredTypeDescriptors are created by FennelStoredTypeDescriptorFactory
 * objects.
 *
 * <p>Each FennelStoredTypeDescriptor has a unique ordinal number assigned to
 * it, which should match across all implementations of tuple libraries that
 * would interact. How to maintain these ordinals in synch is left as an
 * exercise for the reader.
 *
 * <p>NOTE: this interface varies from the C++ implementation by requiring the
 * stored type to know how to create a FennelAttributeAccessor for itself. This
 * seems cleaner than trying to infer it within the standard
 * FennelTupleAccessor. This class is JDK 1.4 compatible.
 *
 * @author Mike Bennett
 * @version $Id$
 */
public interface FennelStoredTypeDescriptor
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the ordinal representing this type.
     */
    public int getOrdinal();

    /**
     * Returns number of bits in marshalled representation, or 0 for a non-bit
     * type; currently only 0 or 1 is supported.
     */
    public int getBitCount();

    /**
     * Returns the width in bytes for a fixed-width non-bit type which admits no
     * per-attribute precision, or 0 for types with per-attribute precision; for
     * bit types, this yields the size of the unmarshalled representation.
     */
    public int getFixedByteCount();

    /**
     * Gets the number of bytes required to store the narrowest value with this
     * type, given a particular max byte count. For a fixed-width type, the
     * return value is the same as the input.
     *
     * @param maxWidth maximum width for which to compute the minimum
     *
     * @return number of bytes
     */
    public int getMinByteCount(int maxWidth);

    /**
     * Gets the alignment size in bytes required for values of this type, given
     * a particular max byte count. This must be 1, 2, 4, or 8, and may not be
     * greater than 1 for variable-width datatypes. For fixed-width datatypes,
     * the width must be a multiple of the alignment size.
     *
     * @param width width for which to compute the alignment
     *
     * @return number of bytes
     */
    public int getAlignmentByteCount(int width);

    /**
     * Creates an FennelAttributeAccessor appropriate for marshalling an element
     * of this type.
     *
     * @return FennelAttributeAccessor
     */
    public FennelAttributeAccessor newAttributeAccessor();

    /**
     * Indicates whether numeric data type is signed.
     *
     * @return false for non-numeric data types, false for unsigned numeric data
     * types, true for signed numeric data types
     */
    public boolean isSigned();

    /**
     * Indicates whether numeric data type is exact.
     *
     * @return false for non-numeric data types, false for approximate numeric
     * data types (REAL, FLOAT) true for exact numeric data types
     */
    public boolean isExact();
}

// End FennelStoredTypeDescriptor.java
