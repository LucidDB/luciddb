/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
// Portions Copyright (C) 1999-2005 John V. Sichi
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

<h3>Overview</h3>
This is a "how-to" document to guide programmers through the
process of adding new instances of ExtendedInstruction
Instruction objects into Calculator.
<a name="Instruction"></a>
<a name="ExtendedInstruction"></a>
<a name="Calculator"></a>

<h3>Adding Extended Instructions</h3>
<p>
Please see the Calculator Technical Specification for details
on how Calculator works internally.
</p>
<p>
ExtendedInstructions are in some sense like call-outs for
Calculator. Simple operations and those that operate on many types
could be templated, might possibly be implemented as regular
instructions instead of ExtendedInstructions.
However, actions that call external libraries or perform complex
operations should almost always be implemented as ExtendedInstructions.
</p>

<h3>Process</h3>
<ol>
<li>
<a name="SqlString.h"></a>
<a name="SqlString.cpp"></a>
<a name="SqlString"></a>
Build a library that implements the desired functionality. For now
this library should reside in fennel/disruptivetech/calc.
Example: SqlString. SqlString.h, SqlString.cpp
</li>
<li>
<a name="SqlStringTest"></a>
<a name="SqlStringTest.cpp"></a>
Unit test the library, extensively if need be, in fennel/test, using
the BOOST framework.  Example:
SqlStringTest. SqlStringTest.cpp.
</li>
<li>
<a name="ExtString"></a>
<a name="ExtString.cpp"></a>
<a name="ExtString.h"></a>
Build ExtendedInstruction wrappers around your library. These should
be in fennel/disruptivetech/calc, and the files should begin with Ext.
Example: ExtString ExtString.h ExtString.cpp
</li>
<li>
<a name="CalcExtStringTest"></a>
Unit test the ExtendedInstruction in fennel/test. This test should
check that all the parameters are being plumbed correctly into your
library, that NULL semantics are handled correctly, etc.
Example: CalcExtStringTest CalcExtStringTest.cpp
</li>
</ol>

<h3>Context</h3>
<p>
An ExtendedInstruction may wish to store a context to reuse on
a subsequent invocation. For example, if an Instruction can perform a
pre-compilation step, and/or cache objects, this information may be
made available the next time the particular ExtendedInstruction
instance is called.
</p>
<p>
A context is associated with each instance of the ExtendedInstruction,
and is not shared across instances or across Calculator instances. For
example, if a new instruction called squareRoot() was created, and
called in two different locations in a program, each call would have a
seperate context.
</p>
<p>
If some context would be useful for all instances of an Instruction,
regardless to which Calculator instance the Instruction belongs,
consider wrapping a singleton or other static object to provide the
back-door access between ExtendedInstruction instances. If some
context would be useful for ExtendedInstruction instances that
belonged to just one Calculator, considerable modifications will have
to be made, perhaps based on the RegisterReference::setCalc() call
that would provide a unique Calculator key.
</p>
<a name="ExtendedInstructionContext"></a>
<a name="~ExtendedInstructionContext"></a>
<p>
ExtendedInstructionContext is an abstract base class, or nearly one,
that can be subclassed to store whatever is needed. When Calculator is
destroyed, the destructor ~ExtendedInstructionContext is called
automatically.
</p>
<a name="CalcExtContext"></a>
<a name="CalcExtContext.cpp"></a>
<p>
An example of using context may be found in CalcExtContextTest and
CalcExtContextTest.cpp
</p>

<h3>Register References</h3>
<a name="RegisterRef"></a>
<a name="RegisterReference"></a>
<a name="TupleData"></a>
<p>
Register sets are an abstraction built upon TupleData objects.
RegisterReference is the generic type. RegisterRef is a templated
subclass that provides accessor functions. This templating provides some
type checking.
</p>
<p> 
Access native types, such as integers, floats, etc. through the
member function RegisterRef::value. 
</p>
<p>
Set a value to NULL with RegisterRef::toNull, check if it is null
with RegisterRef::isNull.
</p>
<p>
Access strings such as VARCHAR, CHAR, BINARY and VARBINARY
with the functions:
<ul>
<li>
RegisterRef::pointer
<li>
RegisterRef::length
<li>
RegisterRef::stringLength
<li>
RegisterRef::storage
</ul>
<p>
Note that length refers to the current length of the string. CHAR
and BINARY strings should always have their length set to their
storage size and be padded correctly once a Calculator program exits.
Variable length strings will always have their length set to a value
less than or equal to the storage for the string. stringLength is
a convenience function for Instructions that can handle both fixed
and variable length strings.
</p>
<p>
Also note that, especially when dealing with strings where setting the
result is a two-phase process, you should leave result variables in a
consistent state if the underlying library should throw an
exception.
</p>

<h3>Types</h3>
<a name="StandardTypeDescriptor"></a>
<a name="StandardTypeDescriptor::toString"></a>
Calculator supports all of the types that are supported by tuples. See
StandardTypeDescriptor and StandardTypeDescriptor::toString for
the mapping between strings and types. This mapping is important in
writing and assembling programs that use ExtendedInstructions, as the
function signatures, see below, are typed.
</p>
<p>
Types CHAR, VARCHAR, BINARY and VARBINARY are all represented by a
RegisterReference<char*>. Calculator makes no attempt to provide
type-safety down to the distinction between CHAR and VARCHAR.
</p>
<p>
Note that you will often have to make two or perhaps four instructions
to support strings. For type checking to work correctly, you will need
an ExtendedInstruction for CHAR, VARCHAR, BINARY and VARBINARY, even
though some or all of these types can be handled implicitly by a
single call into the underlying library.
</p>

<h3>Exceptions</h3>
<p>
If your library comes across a data-driven error condition of any
sort, it should throw an exception or otherwise signal the
ExtendedInstruction that it must, in due course, throw an exception.
Once this sort of exception is thrown, program execution continues, so
some care must be taken to leave the result in a consistent
state. Generally this means setting the result to NULL. (There may be
some issues with columns that are set to NOT NULL here, but the
responsibility for sorting this out should be in the domain of the
program writer.)
</p>
<p>
Nearly all error cases will already be labeled by SQL99 Part 2
Section 22.1. For example, String Data Right Truncation is 22-001, or
22001. Libraries should simply throw a Calculator independent string,
for example "22001".
</p>
<p>
If the library chooses to throw an exception, the ExtendedInstruction
should catch the string, set the result to NULL, and re-throw the
string.  The ExtendedInstruction class will catch this string and
re-throw it with the appropriate Calculator wrappers.  
</p>
<p>
If there is an error condition not handled by the SQL99 spec, a
standardized list of Fennel specific errors should be agreed
upon. Please discuss this with the group, and document well, before
forging ahead.
</p>
<p>
In the unlikely event that an ExtendedInstruction can determine that
further execution of this Calculator and Execution Object (XO) is
hopeless, another exception type could be thrown, bringing down the
whole house of cards.
</p>

<h3>Registering</h3>
<a name="InstructionFactory"></a>
<a name="CalcAssembler"></a>
<a name="ExtStringRegister"></a>
<a name="ExtString.cpp"></a>

<p>
Each new ExtendedInstruction must be registered with the
InstructionFactory so that the CalcAssembler can instantiate the
Instruction. Each Ext-something-.cpp file must have a routine
Ext-something-Register, for example ExtStringRegister in ExtString.cpp
This routine is called once by the singleton CalcInit::instance at the
start of the Fennel library.
</p>
<p>
You will have to edit:
<ul>
<li>
CalcInit.cpp, to add registration to CalcInit
<li>
InstructionCommon.h, to include your Ext-something-.h file.
<li>
calc/Makefile.am, for obvious reasons.
</ul>

<h3>Function Definitions</h3>
<a name="ExtendedInstructionDef"></a>
<a name="ExtendedInstructionDef::computeSignature"></a>
<p>
ExtendedInstruction objects are looked up by CalcAssembler by their
definition.  The details are handled by ExtendedInstructionDef and
ExtendedInstructionDef::computeSignature. A signature is not
explicitly registered by the Register function, but is used by
CalcAssembler to look up the appropriate ExtendedInstruction at
assemble time.
</p>
<p>
A typical signature looks like:
<pre>strCatA2(c,c)</pre>
<a name="ExtendedInstructionTable::add"></a>
<p>
The instruction is called strCatA2. It takes two arguments, both are
of type CHAR. To create this signature, an appropriate
vector<StandardTypeDescriptorOrdinal> will have to be constructed.
This vector, plus a corresponding type, and a pointer to the function
that implements the instruction must be sent to
ExtendedInstructionTable::add.  For example:
</p>
<pre>eit->add("strCatA2", params_2F,
        (ExtendedInstruction2<char*, char*>*) NULL,
         &strCatA2);</pre>
<p>
An assembly program that calls this ExtendedInstruction might look
like:
</p>
<pre>O vc,5, c,5;
C vc,5, c,5;
V 0x414243, 0x4748494a20;
T;
CALL 'strCatA2(O0, C0);</pre>

*/

struct ExtendedInstructionHowTo 
{
        // NOTE:  dummy class for doxygen
};

FENNEL_END_CPPFILE("$Id$");

// End ExtendedInstructionHowTo.cpp

