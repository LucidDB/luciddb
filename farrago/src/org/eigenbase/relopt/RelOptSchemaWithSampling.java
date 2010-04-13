/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package org.eigenbase.relopt;

/**
 * Extension to {@link RelOptSchema} with support for sample datasets.
 *
 * @author jhyde
 * @version $Id$
 * @see RelOptConnection
 * @see RelOptSchema
 */
public interface RelOptSchemaWithSampling
    extends RelOptSchema
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves a {@link RelOptTable} based upon a member access, using a
     * sample dataset if it exists.
     *
     * @param names Compound name of table
     * @param datasetName Name of sample dataset to substitute, if it exists;
     * null to not look for a sample
     * @param usedDataset Output parameter which is set to true if a sample
     * dataset is found; may be null
     *
     * @return Table, or null if not found
     */
    RelOptTable getTableForMember(
        String [] names,
        String datasetName,
        boolean [] usedDataset);
}

// End RelOptSchemaWithSampling.java
