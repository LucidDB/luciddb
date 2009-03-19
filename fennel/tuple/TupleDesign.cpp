/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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

FENNEL_BEGIN_CPPFILE("$Id$");

/**

<a name="Overview"></a>
<h3>Overview</h3>

The Fennel tuple library defines classes for manipulating rows of relational
data and physically mapping them to contiguous byte arrays for storage in
memory or on disk.  The library was designed to meet the following goals:

<ul>

<li> Performance: tuple access performance must be as close as possible to the
equivalent access to a struct understood by the C++ compiler.

<li> Compactness: physical data representations should be as small as possible
for better usage of I/O bandwidth.  This can lead to tradeoffs with the
performance goal (e.g. aligned data can be accessed faster but requires extra
padding).

<li> Completeness: the tuple-level aspects of common relational operations such
as projection and join must be supported efficiently.  Block storage,
streaming, and NULL values must also be supported.

<li> Extensibility: applications must be able to define their own datatypes not
included in the standard Fennel definitions.

</ul>

The tuple library is <em>not</em> intended for directly representing anything
more complicated than rows of atomic data.  In particular, constructs such
as nested tables (e.g. storing line items together with order headers) and
compound types (e.g. a user-defined address type consisting of street, city,
state, and zip fields) are outside the scope of the tuple library.  That's not
to say that the tuple library can't be used to implement these constructs, but
the logic (e.g. to flatten a row of compound types into a row of simple types)
must be programmed at a higher level.

<p>

A number of other Fennel components depend on the tuple library:

<ul>

<li> btree:  for storing keyed tuple entries

<li> calc:  for accessing inputs and outputs and manipulating intermediate
results

<li> exec:  for representing streams of relational data

</ul>


<a name="StoredTypeDescriptor"></a>
<h3>Type System</h3>

All tuple data values must have well-defined type descriptors.  Fennel
abstracts this concept in the StoredTypeDescriptor interface.  A
StoredTypeDescriptor instance defines

<ul>

<li> Storage requirements: Are values fixed-width or variable-width?  In bytes
or bits?  Is alignment required for access?  For variable-width values, the
maximum width is not specified; for example, in different contexts, the same
StoredTypeDescriptor could be used to represent either a VARCHAR(15) or a
VARCHAR(1024).

<li> Value comparison:  a method for comparing two values of this type.

<li> Metadata:  every type must be assigned a unique ordinal value; this is
used for storing tuple type descriptors persistently (e.g. for self-describing
log entries).

<li> Value visitor:  a method allowing tuples to participate in the Visitor
pattern, so that values can be processed generically via instances of
DataVisitor (e.g. table dump).

</ul>

Fennel uses the Factory pattern for creating StoredTypeDescriptor instances.
An application chooses an implementation of interface
StoredTypeDescriptorFactory in order to be able to define tuple data types.
Fennel provides a default implementation in StandardTypeDescriptorFactory,
which defines the following type ordinals (declared in enum
StandardTypeDescriptorOrdinal):

<ul>

<li>STANDARD_TYPE_INT_8: 8-bit signed int

<li>STANDARD_TYPE_UINT_8: 8-bit unsigned int

<li>STANDARD_TYPE_INT_16: 16-bit signed int

<li>STANDARD_TYPE_UINT_16: 16-bit unsigned int

<li>STANDARD_TYPE_INT_32: 32-bit signed int

<li>STANDARD_TYPE_UINT_32: 32-bit unsigned int

<li>STANDARD_TYPE_INT_64: 64-bit signed int

<li>STANDARD_TYPE_UINT_64: 64-bit unsigned int

<li>STANDARD_TYPE_REAL: native floating point

<li>STANDARD_TYPE_DOUBLE: native double precision floating point

<li>STANDARD_TYPE_CHAR: fixed-width single-byte character string

<li>STANDARD_TYPE_VARCHAR: variable-width single-byte character string with SQL
comparison semantics (always effectively right-trimmed before comparison);
no zero-terminator is stored; instead, a separate string length field is
maintained

<li>STANDARD_TYPE_BINARY: fixed-width byte string

<li>STANDARD_TYPE_VARBINARY: variable-width byte string

<li>STANDARD_TYPE_BOOL: single-bit boolean; these are packed 8 at a time
into bytes (along with null indicators)

<li>STANDARD_TYPE_UNICODE_CHAR: fixed-width double-byte (UCS-2) character string

<li>STANDARD_TYPE_UNICODE_VARCHAR: variable-width double-byte (UCS-2) character
string with SQL comparison semantics

</ul>

Applications may extend this list with their own types via a customized
subclass of StandardTypeDescriptorFactory.  Primitive types may also
be reinterpreted at higher levels; e.g. date/time types are often
represented as a STANDARD_TYPE_UINT_64 interpreted as a quantity such as
milliseconds since a given epoch.

<p>

TODO: lobs

<a name="TupleDescriptor"></a>
<h3>Tuple Descriptors</h3>

A StoredTypeDescriptor describes a homogeneous domain of possible data values
for a single attribute (e.g. a column in a table); several of these can be
combined into an ordered, heterogeneous tuple descriptor via the
TupleDescriptor and TupleAttributeDescriptor classes.  A single
TupleAttributeDescriptor specifies:

<ul>

<li>the StoredTypeDescriptor describing values to be stored

<li>a maximum storage length in bytes; this is implied by the
StoredTypeDescriptor for fixed-width types

<li>whether NULL values are possible

</ul>

TupleDescriptor is simply a std::vector of TupleAttributeDescriptors (with some
extra utility methods tacked on); this makes it very easy to manipulate
TupleDescriptors directly via STL.  For example, tuple descriptors can be
concatenated with code like

\verbatim
    TupleDescriptor td1 = getFirstTupleDesc();
    TupleDescriptor td2 = getSecondTupleDesc();
    td1.insert(td1.end(),td2.begin(),td2.end());
    return td1;
\endverbatim

Consider an SQL statement like

\verbatim
create table MACHINES(
    IP_ADDRESS int not null,
    NAME varchar(32));
\endverbatim

A TupleDescriptor to define the rows of this table could be constructed as:

\verbatim
    StandardTypeDescriptorFactory typeFactory;
    TupleDescriptor machineTupleDesc;
    TupleAttributeDescriptor ipAddressAttr(
        typeFactory.newDataType(STANDARD_TYPE_UINT32),
        false,
        0);
    TupleAttributeDescriptor nameAttr(
        typeFactory.newDataType(STANDARD_TYPE_VARCHAR),
        true,
        32);
    machineTupleDesc.push_back(ipAddressAttr);
    machineTupleDesc.push_back(nameAttr);
\endverbatim

Note that the TupleDescriptor contains only a minimal amount of information
needed for data storage; it does not store metadata such as attribute names.
While a TupleDescriptor does specify the logical structure of rows to be
stored, it does not contain the data itself, nor does it explicitly specify
the physical layout of data values.  The next two sections describe additional
classes which work together with TupleDescriptor to carry out those duties.


<a name="TupleData"></a>
<h3>Discontiguous Tuple Data Representation</h3>

A row of data to be processed can be represented in memory with all of the data
values stored at discontiguous addresses; this is the job of the TupleData
class, which is a std::vector of TupleDatum instances.  A TupleDatum is a
combination of a pointer to the actual data together with a byte length.  A
NULL data pointer is interpreted as a NULL value.  To continue the previous
example, a row to be inserted into the MACHINES table could be constructed like
so:

\verbatim
    TupleData machineTupleData(machineTupleDesc);
    uint32_t localhostIP = 0x7F000001;
    char const *pMachineName = "jackalope";
    machineTupleData[0].pData = &amp;localhostIP;
    machineTupleData[1].pData = pMachineName;
    machineTupleData[1].cbData = strlen(pMachineName);
\endverbatim

Notes:

<ul>

<li>By virtue of derivation from std::vector, the TupleData class automatically
manages memory for its contained TupleDatum instances.  However, it does
<em>not</em> manage memory for the data values themselves.  The addresses
&localHostIP and pMachineName must remain valid for as long as machineTupleData
is to be accessed.

<li>The TupleData constructor takes a TupleDescriptor parameter.  This allows
for the automatic allocation of the same number of TupleDatum instances as the
TupleDescriptor has TupleAttributeDescriptors.  It also sets up the cbData
field of all fixed-width fields to the correct value.  In this example,
machineTupleData[0].cbData is automatically set to sizeof(uint32_t).  This
allows tuple-processing code to use memcpy to blindly transfer data values.

<li>TupleData also supports a default constructor which results in a 0-length
vector.  This can be used in cases where TupleData instances are to be
constructed independent of a TupleDescriptor, or when the TupleDescriptor isn't
available yet (the TupleData::compute method can be used later when the
TupleDescriptor becomes available).

<li>Typically, TupleData instances are not 1-to-1 with tuple instances as in
this example.  Instead a single TupleData instance is typically constructed
just once and then used over and over to reference many different tuple
instances, all of which conform to the original TupleDescriptor.

</ul>


<a name="TupleAccessor"></a>
<h3>Stored Tuple Access</h3>

So far, TupleDescriptor and TupleData by themselves aren't terribly useful.
The TupleAccessor class is the missing piece which allows tuple data to be
efficiently stored and accessed in contiguous memory buffers.  As with
TupleData, the idea is that a TupleAccessor can be precomputed from a
TupleDescriptor once, and then used over and over again to access many
different stored tuples.  The most important operations on a TupleAccessor once
it has been computed are:

<ul>

<li>marshal:  given a TupleData instance, gather its discontiguous data values
into a contiguous buffer for storage.  The TupleData instance is not modified.

<li>unmarshal: given a contiguous buffer previously written by a marshal() call
from an identical TupleAccessor, update a TupleData instance to reference the
stored data (but do not actually copy the data values).  The buffer is not
modified.  After an unmarshal() call, the caller can process the individual
TupleDatum entries one by one.

</ul>

In the context of the running example:

\verbatim
void storeMachineTuple(FILE *file)
{
    TupleAccessor tupleAccessor;
    tupleAccessor.compute(machineTupleDesc);
    boost::scoped_array<byte> tupleBuffer =
        new byte[tupleAccessor.getMaxByteCount()];
    tupleAccessor.marshal(
        machineTupleData,
        tupleBuffer.get());
    fwrite(
        tupleBuffer.get(),
        1,
        tupleAccessor.getCurrentByteCount(),
        file);
}

uint32_t readStoredMachineIpAddress(FILE *file)
{
    TupleAccessor tupleAccessor;
    tupleAccessor.compute(machineTupleDesc);
    boost::scoped_array<byte> tupleBuffer =
        new byte[tupleAccessor.getMaxByteCount()];
    fread(
        tupleBuffer.get(),
        1,
        tupleAccessor.getMaxByteCount(),
        file);
    tupleAccessor.setCurrentTupleBuf(tupleBuffer.get());
    tupleAccessor.unmarshal(machineTupleData);
    return *((uint32_t *) (machineTupleData[0].pData));
}
\endverbatim

The diagram below shows the effect of the marshal and unmarshal operations.
The gray boxes are internal length-indicator fields described later on:

<hr>
\image html TupleAccess.gif
<hr>

<h3>Contiguous Tuple Data Representation</h3>

TupleAccessor's physical storage scheme is designed to meet the following
requirements:

<ul>

<li>all values are accessible in constant time

<li>values requiring alignment are always stored at the correct
(platform-specific) alignment boundaries with respect to the start of the
tuple

<li>the total tuple width must be a multiple of the maximum alignment
size for the platform

<li>a null can be stored for any attribute, regardless of datatype

<li>both fixed-width and variable-width values can be stored;
for variable-width aligned values (e.g. UNICODE strings), the only
alignment supported is 2-byte

<li>any bit pattern can be stored (no reserved values for representing
nulls or terminators)

</ul>

A tuple is formatted as an ordered series of fields.  Some of these fields
represent value data; others represent extra information about a value such
as its length or null indicator.  Physical field ordering does not match
TupleDescriptor ordering, although within an attribute storage class (e.g. all
4-byte aligned attributes), the values are stored in the order the
corresponding attributes appear in the TupleDescriptor.  Each
nullable attribute has a corresponding bit field in a bit array.  Bit-typed
values are also stored packed into the same bit array.

<p>

Every field has an offset relative to the start of the stored tuple.  Some
fields are stored at fixed offsets; others have variable offsets.  For
constant-time access to a field with a variable offset, it is necessary to
compute the variable offset based on the contents of some other field with a
fixed offset.  Such a fixed-offset field containing information about a
variable-offset field is referred to as an indirection field.  Each
variable-width attribute gets a corresponding "end offset" indirection field,
which references the byte after the last data byte of the variable-width
attribute.  Start offsets are not actually recorded, since they can be
computed from the end offset of other attributes.

<p>

The physical field ordering is illustrated in the following diagram:

<hr>
\image html TupleFormat.gif
<hr>

In the variable-width portion, all 2-byte aligned fields are stored before any
unaligned fields; also, if the bitfields end on an odd byte boundary,
one extra byte of padding is inserted so that the first variable width
field comes out aligned.

<h3>Alternate Storage Formats</h3>

In the default representation described above, integer fields are stored in
native byte order.  TupleAccessor supports an alternate format in which
fields are stored in network byte order.  This can be requested by
passing TUPLE_FORMAT_NETWORK as the optional second parameter when
the TupleAccessor is computed.  Since values are unmarshalled by reference
rather than copied, this leads to a question:  where to put the native
unmarshalled value when a network-ordered attribute is accessed?  This
is the purpose of the anonymous union in class TupleDatum.  Unmarshalling
writes the native value to the appropriate field of the union, and then
sets TupleDatum.pData to reference it.

<p>

A third storage format is also supported:
TUPLE_FORMAT_ALL_FIXED.  This format treats all values as fixed-width
(a variable-width attribute is taken at its maximum width).  It is mostly
useful for setting up a TupleData instance with a preallocated staging area.
For example:

\verbatim
void storeLocalhostMachineTuple(FILE *file)
{
    TupleAccessor fixedAccessor;
    fixedAccessor.compute(
        machineTupleDesc,
        TUPLE_FORMAT_ALL_FIXED);
    boost::scoped_array<byte> tupleBuffer =
        new byte[fixedAccessor.getMaxByteCount()];
    // the 2nd arg 'valid = false' tells the accessor not to load the offsets
    // and bitflags  from the new buffer, because they contain garbage.
    fixedAccessor.setCurrentTupleBuf(tupleBuffer.get(), false);

    // this is a little weird; the purpose of this unmarshal isn't actually
    // to read existing data values; instead, it's to set machineTupleData
    // value pointers to the correct offsets in tupleBuffer
    fixedAccessor.unmarshal(machineTupleData);

    fillLocalhostMachineTuple();
    storeMachineTuple(file);
}

void fillLocalhostMachineTuple()
{
    *((uint32_t *) (machineTupleData[0].pData)) = 0x7F000001;
    int rc = gethostname((char *) machineTupleData[1].pData,32);
    if (rc == -1) {
        // couldn't get host name; null it out
        machineTupleData.pData = NULL;
    }
}
\endverbatim

<p>

Storage formats are defined in enum TupleFormat.

<a name="TupleProjection"></a>
<h3>Tuple Projection</h3>

It was previously mentioned that access to any individual value
in a stored tuple can be performed in constant time.  This makes no
difference when the entire tuple is unmarshalled at once.  However,
the tuple library also provides classes for accessing a projection
(a subset of the attributes of the tuple, possibly reordered).
Analogously to the way TupleDescriptor and
TupleAccessor work together, class TupleProjection defines the logical
projection (as a std::vector of 0-based attribute positions),
while class TupleProjectionAccessor defines how to extract it.

Suppose we want to extract the NAME attribute from a series of stored tuples.
The following class does the trick:

\verbatim
class NameExtractor
{
    TupleAccessor tupleAccessor;
    TupleProjection proj;
    TupleDescriptor projDescriptor;
    TupleProjectionAccessor projAccessor;
    TupleData projData;

public:
    explicit NameExtractor()
    {
        // the name field is attribute #1 counting from 0
        proj.push_back(1);
        tupleAccessor.compute(machineTupleDesc);
        projAccessor.bind(machineTupleAccessor,proj);
        projDescriptor.projectFrom(machineTupleDesc,proj);
        projData.compute(projDescriptor);
    }

    char const *getMachineName(byte const *pStoredTuple)
    {
        tupleAccessor.setCurrentTupleBuf(pStoredTuple);
        projAccessor.unmarshal(projData);
        return (char const *) (projData[0].pData);
    }
};
\endverbatim

Notes:

<ul>

<li>projAccessor is "bound" to tupleAccessor.  This is why after we
call tupleAccessor.setCurrentTupleBuf in getMachineName, projAccessor
knows where to find the desired data.  It also means that you have to
be careful about the lifetimes of the two objects.

<li>projDescriptor is a TupleDescriptor describing just the projection
(the name attribute by itself).  TupleData contains a single
corresponding TupleDatum.  That's why the last line of getMachineName uses
projData[0], not projData[1]; after
the projection, the name is the first and only value.

</ul>

<h3>Tuple Comparison</h3>

TupleDescriptor provides method compareTuples for comparing two instances of
TupleData, with the first attribute being the most significant and the last
attribute the least significant in the comparison.  This will probably be
eliminated eventually; a calculator program generator should be used instead.


<h3>TuplePrinter</h3>

The TuplePrinter class provides a simple means of rendering tuple values
as text.

 */
struct TupleDesign
{
    // NOTE:  dummy class for doxygen
};

FENNEL_END_CPPFILE("$Id$");

// End TupleDesign.cpp
