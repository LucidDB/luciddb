/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
package org.eigenbase.jmi;

/**
 * Special exception to flag a reference to an unvalidated dependency. When such
 * a dependency is detected (usually in the context of a cyclic definition), we
 * throw this exception to terminate processing of the current object. The
 * JmiChangeSet catches it and recovers, marking the object as needing another
 * try, and moves on to other objects. If validation reaches a fixpoint, it
 * means there is an object definition cycle (which is illegal).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiUnvalidatedDependencyException
    extends RuntimeException
{
}

// End JmiUnvalidatedDependencyException.java
