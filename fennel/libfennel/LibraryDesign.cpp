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
