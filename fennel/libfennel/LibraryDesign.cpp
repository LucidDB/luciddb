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

FENNEL_BEGIN_CPPFILE("$Id$");

/**

<h3>Fennel Architecture</h3>

The diagram below specifies the layering of Fennel components:

<hr>

\image html FennelArch.gif

<hr>

In general, higher-level components depend on lower-level components,
but not vice versa (e.g. everything depends on common, and btree and
exec both depend on tuple).  No lateral dependencies are allowed
(e.g. segment does not depend on tuple).  In this context, dependency
means early binding; that is, the higher-level component references
the lower-level component in order to satisfy preprocessor include directives
and/or the linker (whether static or dynamic).

<p>

Other aspects of the diagram are suggestive but not strict.  For
example, the segment layer mostly "hides" the cache and device
layers, so that higher layers depend on segment to pass on requests to
cache and device.  But in some cases, they may bypass segment and
access a device directly.  Similarly, the thirdparty boost and STLport
libraries are available throughout Fennel.  Finally, note that
although common and synch are separate source trees they are compiled
into a single library called common.

<p>

The build process enforces the layering by building each layer as a
separate shared library and requiring dependencies to be linked
explicitly.  Layering violations will result in unresolved externals
during link.

<p>

Note that late binding is allowed to violate the layering.  For
example, the cache layer defines and calls the interface
MappedPageListener.  The segment layer defines classes which implement
MappedPageListener, and passes instances of them to the cache layer.
At runtime, this means code in the cache layer ends up calling code in
the segment layer.

<hr>

 */
struct LibraryDesign
{
    // NOTE:  dummy class for doxygen
};

FENNEL_END_CPPFILE("$Id$");

// End LibraryDesign.cpp
