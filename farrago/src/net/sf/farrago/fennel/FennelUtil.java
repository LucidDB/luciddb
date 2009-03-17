/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
import org.eigenbase.util.*;

import java.nio.charset.*;

/**
 * Static utility methods related to Fennel storage. Historically, these methods
 * were refactored from FennelRelUtil to remove their dependency on the Farrago
 * query package.
 *
 * @author John Pham
 * @version $Id$
 */
public abstract class FennelUtil
{
    //~ Methods ----------------------------------------------------------------

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
                    convertSqlTypeToFennelType(type),
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
     * <td>{@link SqlTypeName#BOOLEAN}</td>
     * <td>{@link FennelStandardTypeDescriptor#BOOL BOOL}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#TINYINT}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_8 INT_8}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#SMALLINT}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_16 INT_16}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#INTEGER}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_32 INT_32}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#DECIMAL}(precision, scale)</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>
     * <p>We use a scaled integer representation. For example, the <code>
     * DECIMAL(6, 2)</code> value 1234.5 would be represented as an {@link
     * FennelStandardTypeDescriptor#INT_32 INT_32} value 123450 (which is 1234.5
     * 10 ^ 2)</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#DATE}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>Milliseconds since the epoch.</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#TIME}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>Milliseconds since midnight.</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#TIMESTAMP}</td>
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
     * <td>{@link SqlTypeName#INTERVAL_DAY_TIME}</td>
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
     * <td>{@link SqlTypeName#INTERVAL_YEAR_MONTH}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>Not implemented.
     *
     * <p>All types of year-month interval are represented in the same way: an
     * integer value which holds the number of months. For example, <code>
     * INTERVAL '2' YEAR</code> and <code>INTERVAL '24' MONTH</code> are both
     * represented as 24.
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#BIGINT}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#VARCHAR}(precision)</td>
     * <td>{@link FennelStandardTypeDescriptor#VARCHAR VARCHAR}
     * or {@link FennelStandardTypeDescriptor#VARCHAR UNICODE_VARCHAR}
     * depending on character set</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#VARBINARY}(precision)</td>
     * <td>{@link FennelStandardTypeDescriptor#VARBINARY VARBINARY}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#MULTISET}</td>
     * <td>{@link FennelStandardTypeDescriptor#VARBINARY VARBINARY}</td>
     * <td>The fields are serialized into the VARBINARY field in the standard
     * Fennel serialization format. There is no 'count' field. To deduce the
     * number of records, deserialize values until you reach the length of the
     * field. Of course, this requires that every value takes at least one byte.
     *
     * <p>The length of a multiset value is limited by the capacity of the
     * <code>VARBINARY</code> datatype. This limitation will be lifted when
     * <code>LONG VARBINARY</code> is implemented.</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#ROW}</td>
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
     * <td>{@link SqlTypeName#CHAR}(precision)</td>
     * <td>{@link FennelStandardTypeDescriptor#CHAR CHAR}
     * or {@link FennelStandardTypeDescriptor#CHAR CHAR}
     * depending on character set</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#BINARY}(precision)</td>
     * <td>{@link FennelStandardTypeDescriptor#BINARY BINARY}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#REAL}</td>
     * <td>{@link FennelStandardTypeDescriptor#REAL REAL}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#FLOAT}</td>
     * <td>{@link FennelStandardTypeDescriptor#DOUBLE DOUBLE}</td>
     * <td>&nbsp;</td>
     * </tr>
     * <tr>
     * <td>{@link SqlTypeName#DOUBLE}</td>
     * <td>{@link FennelStandardTypeDescriptor#DOUBLE DOUBLE}</td>
     * <td>&nbsp;</td>
     * </tr>
     * </table>
     */
    public static FennelStandardTypeDescriptor convertSqlTypeToFennelType(
        RelDataType sqlType)
    {
        switch (sqlType.getSqlTypeName()) {
        case BOOLEAN:
            return FennelStandardTypeDescriptor.BOOL;
        case TINYINT:
            return FennelStandardTypeDescriptor.INT_8;
        case SMALLINT:
            return FennelStandardTypeDescriptor.INT_16;
        case INTEGER:
            return FennelStandardTypeDescriptor.INT_32;
        case DATE:
        case TIME:
        case TIMESTAMP:
        case BIGINT:
        case INTERVAL_DAY_TIME:
        case INTERVAL_YEAR_MONTH:
            return FennelStandardTypeDescriptor.INT_64;
        case VARCHAR:
            if (SqlTypeUtil.isUnicode(sqlType)) {
                return FennelStandardTypeDescriptor.UNICODE_VARCHAR;
            } else {
                return FennelStandardTypeDescriptor.VARCHAR;
            }
        case VARBINARY:
        case MULTISET:
            return FennelStandardTypeDescriptor.VARBINARY;
        case CHAR:
            if (SqlTypeUtil.isUnicode(sqlType)) {
                return FennelStandardTypeDescriptor.UNICODE_CHAR;
            } else {
                return FennelStandardTypeDescriptor.CHAR;
            }
        case BINARY:
            return FennelStandardTypeDescriptor.BINARY;
        case REAL:
            return FennelStandardTypeDescriptor.REAL;
        case DECIMAL:
            return FennelStandardTypeDescriptor.INT_64;
        case FLOAT:
        case DOUBLE:
            return FennelStandardTypeDescriptor.DOUBLE;
        default:
            throw Util.unexpected(sqlType.getSqlTypeName());
        }
    }
}

// End FennelUtil.java
