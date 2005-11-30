/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package com.lucidera.farrago.namespace.flatfile;

import java.sql.*;
import java.util.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.rel.jdbc.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * FlatFileColumnSet provides a flatfile implementation of the {@link
 * FarragoMedColumnSet} interface.
 *
 * @author John V. Pham
 * @version $Id$
 */
class FlatFileColumnSet extends MedAbstractColumnSet
{
    //~ Instance fields -------------------------------------------------------

    FlatFileParams params;
    String filename;

    //~ Constructors ----------------------------------------------------------

    FlatFileColumnSet(
        String [] localName,
        RelDataType rowType,
        FlatFileParams params,
        String filename)
    {
        super(localName, null, rowType, null, null);
        this.params = params;
        this.filename = filename;
    }

    //~ Methods ---------------------------------------------------------------

    public FlatFileParams getParams() 
    {
        return params;
    }

    public String getFilename()
    {
        return filename;
    }
        
    // implement RelOptTable
    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        return new FlatFileFennelRel(this, cluster, connection);
    }
}


// End FlatFileColumnSet.java
