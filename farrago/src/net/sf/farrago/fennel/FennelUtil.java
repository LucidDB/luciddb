/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.fennel;

import net.sf.farrago.fennel.tuple.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;

/**
 * Static utility methods related to Fennel storage. Historically, these 
 * methods were refactored from FennelRelUtil to remove their dependency 
 * on the Farrago query package.
 * 
 * @author John Pham
 * @version $Id$
 */
public abstract class FennelUtil
{
    /**
     * Creates a FennelTupleDescriptor for a RelDataType which is a row.
     *
     * @param rowType row type descriptor
     *
     * @return generated tuple descriptor
     */
    public static FennelTupleDescriptor convertRowTypeToFennelTupleDesc(
        RelDataType rowType)
    {
        FennelTupleDescriptor tupleDesc = new FennelTupleDescriptor();
        for (RelDataTypeField field : rowType.getFields()) {
            RelDataType type = field.getType();
            FennelTupleAttributeDescriptor attrDesc =
                new FennelTupleAttributeDescriptor(
                    FennelUtil.convertSqlTypeNameToFennelType(
                        type.getSqlTypeName()),
                    type.isNullable(),
                    SqlTypeUtil.getMaxByteSize(type));
            tupleDesc.add(attrDesc);
        }
        return tupleDesc;
    }

    /**
     * Converts a SQL type to a Fennel type.
     *
     * <p>The mapping is as follows:
     *
     * <table border="1">
     * <tr>
     * <th>SQL type</th>
     * <th>Fennel type</th>
     * <th>Comments</th>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Boolean}</td>
     * <td>{@link FennelStandardTypeDescriptor#BOOL BOOL}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Tinyint}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_8 INT_8}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Smallint}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_16 INT_16}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Integer}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_32 INT_32}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Decimal}(precision, scale)</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>
     * <p>We plan to use a shifted representation. For example, the <code>
     * DECIMAL(6, 2)</code> value 1234.5 would be represented as an {@link
     * FennelStandardTypeDescriptor#INT_32 INT_32} value 123450 (which is 1234.5
     * 10 ^ 2)</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Date}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>Milliseconds since the epoch.</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Time}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>Milliseconds since midnight.</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Timestamp}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>Milliseconds since the epoch.</td>
     * </tr>
     * <tr>
     * <td>Timestamp with timezone</td>
     * <td>&nbsp;</td>
     * <td>Not implemented. We will probably use a user-defined type consisting
     * of a TIMESTAMP and a VARCHAR.</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#IntervalDayTime}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>Not implemented.
     *
     * <p>All types of day-time interval are represented in the same way: an
     * integer milliseconds value. For example, <code>INTERVAL '1' HOUR</code>
     * and <code>INTERVAL '3600' MINUTE</code> are both represented as
     * 3,600,000.
     *
     * <p>TBD: How to represent fractions of seconds smaller than a millisecond,
     * for example, <code>INTERVAL SECOND(6)</code>.</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#IntervalYearMonth}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>Not implemented.
     *
     * <p>All types of year-month interval are represented in the same way: an
     * integer value which holds the number of months. For example, <code>
     * INTERVAL '2' YEAR</code> and <code>INTERVAL '24' MONTH</code> are both
     * represented as 24.
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Bigint}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Varchar}(precision)</td>
     * <td>{@link FennelStandardTypeDescriptor#VARCHAR VARCHAR}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Varbinary}(precision)</td>
     * <td>{@link FennelStandardTypeDescriptor#VARBINARY VARBINARY}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Multiset}</td>
     * <td>{@link FennelStandardTypeDescriptor#VARBINARY VARBINARY}</td>
     * <td>The fields are serialized into the VARBINARY field in the standard
     * Fennel serialization format. There is no 'count' field. To deduce the
     * number of records, deserialize values until you reach the length of the
     * field. Of course, this requires that every value takes at least one byte.
     *
     * <p>The length of a multiset value is limited by the capacity of the
     * <code>VARBINARY</code> datatype. This limitation will be liften when
     * <code>LONG VARBINARY</code> is implemented.</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Row}</td>
     * <td>&nbsp;</td>
     * <td>The fields are 'flattened' so that they become top-level fields of
     * the relation.
     *
     * <p>If the row is nullable, then all fields will be nullable after
     * flattening. An extra 'null indicator' field is added to discriminate
     * between a NULL row and a not-NULL row which happens to have all fields
     * NULL.</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Char}(precision)</td>
     * <td>{@link FennelStandardTypeDescriptor#CHAR CHAR}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Binary}(precision)</td>
     * <td>{@link FennelStandardTypeDescriptor#BINARY BINARY}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Real}</td>
     * <td>{@link FennelStandardTypeDescriptor#REAL REAL}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Float}</td>
     * <td>{@link FennelStandardTypeDescriptor#DOUBLE DOUBLE}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#Double}</td>
     * <td>{@link FennelStandardTypeDescriptor#DOUBLE DOUBLE}</td>
     * <td>&nbsp;</td>
     * </tr>
     * </table>
     */
    public static FennelStandardTypeDescriptor convertSqlTypeNameToFennelType(
        SqlTypeName sqlType)
    {
        switch (sqlType.getOrdinal()) {
        case SqlTypeName.Boolean_ordinal:
            return FennelStandardTypeDescriptor.BOOL;
        case SqlTypeName.Tinyint_ordinal:
            return FennelStandardTypeDescriptor.INT_8;
        case SqlTypeName.Smallint_ordinal:
            return FennelStandardTypeDescriptor.INT_16;
        case SqlTypeName.Integer_ordinal:
            return FennelStandardTypeDescriptor.INT_32;
        case SqlTypeName.Date_ordinal:
        case SqlTypeName.Time_ordinal:
        case SqlTypeName.Timestamp_ordinal:
        case SqlTypeName.Bigint_ordinal:
        case SqlTypeName.IntervalDayTime_ordinal:
        case SqlTypeName.IntervalYearMonth_ordinal:
            return FennelStandardTypeDescriptor.INT_64;
        case SqlTypeName.Varchar_ordinal:
            return FennelStandardTypeDescriptor.VARCHAR;
        case SqlTypeName.Varbinary_ordinal:
        case SqlTypeName.Multiset_ordinal:
            return FennelStandardTypeDescriptor.VARBINARY;
        case SqlTypeName.Char_ordinal:
            return FennelStandardTypeDescriptor.CHAR;
        case SqlTypeName.Binary_ordinal:
            return FennelStandardTypeDescriptor.BINARY;
        case SqlTypeName.Real_ordinal:
            return FennelStandardTypeDescriptor.REAL;
        case SqlTypeName.Decimal_ordinal:
            return FennelStandardTypeDescriptor.INT_64;
        case SqlTypeName.Float_ordinal:
        case SqlTypeName.Double_ordinal:
            return FennelStandardTypeDescriptor.DOUBLE;
        default:
            throw sqlType.unexpected();
        }
    }
}

// End FennelUtil.java
