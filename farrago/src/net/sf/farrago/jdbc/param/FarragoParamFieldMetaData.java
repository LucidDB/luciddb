/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
// Portions Copyright (C) 2006-2006 John V. Sichi
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
package net.sf.farrago.jdbc.param;

import java.io.*;

import java.sql.*;


/**
 * This defines the per parameter field metadata required by the client-side
 * driver to implement the JDBC ParameterMetaData API. This class is JDK 1.4
 * compatible.
 *
 * @author Angel Chang
 * @version $Id$
 * @see net.sf.farrago.jdbc.engine.FarragoParamFieldMetaDataFactory
 */
public class FarragoParamFieldMetaData
    implements Serializable
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * SerialVersionUID created with JDK 1.5 serialver tool.
     */
    private static final long serialVersionUID = 5042520840301805755L;

    //~ Instance fields --------------------------------------------------------

    /**
     * SQL paramMetaData of this field.
     */
    public int type;

    /**
     * SQL className.
     */
    public String className;

    /**
     * SQL typename of this field.
     */
    public String typeName;

    /**
     * precision of this field.
     */
    public int precision;

    /**
     * scale of this field.
     */
    public int scale;

    /**
     * indicates whether this parameter field is nullable. One of {{@link
     * java.sql.ParameterMetaData#parameterNoNulls}, {@link
     * java.sql.ParameterMetaData#parameterNullable}, {@link
     * java.sql.ParameterMetaData#parameterNullableUnknown}}.
     */
    public int nullable = ParameterMetaData.parameterNullableUnknown;

    /**
     * indicates whether this field is signed.
     */
    public boolean signed;

    /**
     * indicate the parameter mode. One of {{@link
     * java.sql.ParameterMetaData#parameterModeUnknown}, {@link
     * java.sql.ParameterMetaData#parameterModeIn}, {@link
     * java.sql.ParameterMetaData#parameterModeOut}, {@link
     * java.sql.ParameterMetaData#parameterModeInOut}.
     */
    public int mode = ParameterMetaData.parameterModeUnknown;

    /**
     * String describing the parameter type Usually of the form: typeName
     * (precision, scale)
     */
    public String paramTypeStr;
}
;

// End FarragoParamFieldMetaData.java
