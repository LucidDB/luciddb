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

import java.math.*;

import java.sql.*;

import java.util.Calendar;

import net.sf.farrago.fennel.tuple.*;

import org.eigenbase.util14.*;


/**
 * FarragoJdbcFennelTupleParamDef represents a parameter associated with a
 * FennelTupleDatum. It handles data converstions to the target type. This class
 * is JDK 1.4 compatible.
 *
 * @author Angel Chang
 * @version $Id$
 * @since March 3, 2006
 */
public class FarragoJdbcFennelTupleParamDef
    extends FarragoJdbcParamDef
{
    //~ Instance fields --------------------------------------------------------

    /* ParamDef (non fennel version) used to override scrubValue */
    private FarragoJdbcParamDef defaultParamDef;

    protected Number min;
    protected Number max;

    //~ Constructors -----------------------------------------------------------

    public FarragoJdbcFennelTupleParamDef(
        String paramName,
        FarragoParamFieldMetaData param,
        FarragoJdbcParamDef paramDef)
    {
        super(paramName, param);
        defaultParamDef = paramDef;

        switch (paramMetaData.type) {
        case Types.TINYINT:
            min = NumberUtil.MIN_BYTE;
            max = NumberUtil.MAX_BYTE;
            break;
        case Types.SMALLINT:
            min = NumberUtil.MIN_SHORT;
            max = NumberUtil.MAX_SHORT;
            break;
        case Types.INTEGER:
            min = NumberUtil.MIN_INTEGER;
            max = NumberUtil.MAX_INTEGER;
            break;
        case Types.BIGINT:
            min = NumberUtil.MIN_LONG;
            max = NumberUtil.MAX_LONG;
            break;
        case Types.NUMERIC:
        case Types.DECIMAL:
            min = NumberUtil.getMinUnscaled(paramMetaData.precision);
            max = NumberUtil.getMaxUnscaled(paramMetaData.precision);
            break;
        case Types.BIT:
        case Types.BOOLEAN:
            min = NumberUtil.INTEGER_ZERO;
            max = NumberUtil.INTEGER_ONE;
            break;
        case Types.REAL:
            min = NumberUtil.MIN_FLOAT;
            max = NumberUtil.MAX_FLOAT;
            break;
        case Types.FLOAT:
        case Types.DOUBLE:
            min = NumberUtil.MIN_DOUBLE;
            max = NumberUtil.MAX_DOUBLE;
            break;
        }
    }

    //~ Methods ----------------------------------------------------------------

    public Object scrubValue(Object obj)
    {
        if (defaultParamDef != null) {
            return defaultParamDef.scrubValue(obj);
        } else {
            return super.scrubValue(obj);
        }
    }

    public Object scrubValue(Object obj, Calendar cal)
    {
        if (defaultParamDef != null) {
            return defaultParamDef.scrubValue(obj, cal);
        } else {
            return super.scrubValue(obj, cal);
        }
    }

    public void setNull(FennelTupleDatum datum)
    {
        checkNullable();
        datum.reset();
    }

    public void setBoolean(FennelTupleDatum datum, boolean b)
    {
        switch (paramMetaData.type) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
            datum.setLong(b ? 1 : 0);
            break;
        case Types.NUMERIC:
        case Types.DECIMAL:
            datum.setLong(
                b ? NumberUtil.powTen(paramMetaData.scale).longValue() : 0);
            break;
        case Types.BIT:
        case Types.BOOLEAN:
            datum.setBoolean(b);
            break;
        case Types.REAL:
            datum.setFloat(b ? 1 : 0);
            break;
        case Types.FLOAT:
        case Types.DOUBLE:
            datum.setDouble(b ? 1 : 0);
            break;
        case Types.VARCHAR:
        case Types.CHAR:
            setString(
                paramMetaData.type == Types.CHAR,
                datum,
                b ? "true" : "false",
                Boolean.class);
            break;
        default:
            throw newInvalidType(Boolean.class);
        }
    }

    public void setByte(FennelTupleDatum datum, byte val)
    {
        setLong(datum, (long) val, Byte.class);
    }

    public void setShort(FennelTupleDatum datum, short val)
    {
        setLong(datum, (long) val, Short.class);
    }

    public void setInt(FennelTupleDatum datum, int val)
    {
        setLong(datum, (long) val, Integer.class);
    }

    public void setLong(FennelTupleDatum datum, long val)
    {
        setLong(datum, (long) val, Long.class);
    }

    private void setLong(FennelTupleDatum datum, long val, Class clazz)
    {
        switch (paramMetaData.type) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
            checkRange(
                val,
                min.longValue(),
                max.longValue());
            datum.setLong((long) val);
            break;
        case Types.NUMERIC:
        case Types.DECIMAL:
            BigDecimal bd =
                NumberUtil.rescaleBigDecimal(
                    BigDecimal.valueOf(val),
                    paramMetaData.scale);
            checkRange(
                bd.unscaledValue(),
                (BigInteger) min,
                (BigInteger) max);
            datum.setLong(bd.unscaledValue().longValue());
            break;
        case Types.BIT:
        case Types.BOOLEAN:
            datum.setBoolean(val != 0);
            break;
        case Types.REAL:
            datum.setFloat((float) val);
            break;
        case Types.FLOAT:
        case Types.DOUBLE:
            datum.setDouble((double) val);
            break;
        case Types.VARCHAR:
        case Types.CHAR:
            setString(
                paramMetaData.type == Types.CHAR,
                datum,
                Long.toString(val),
                clazz);
            break;
        default:
            throw newInvalidType(clazz);
        }
    }

    public void setFloat(FennelTupleDatum datum, float val)
    {
        setDouble(datum, (double) val, true);
    }

    public void setDouble(FennelTupleDatum datum, double val)
    {
        setDouble(datum, (double) val, false);
    }

    private void setDouble(FennelTupleDatum datum,
        double val,
        boolean isFloat)
    {
        Class clazz;
        if (isFloat) {
            clazz = Float.class;
        } else {
            clazz = Double.class;
        }
        switch (paramMetaData.type) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
            long n = NumberUtil.round(val);
            checkRange(
                n,
                min.longValue(),
                max.longValue());
            datum.setLong(n);
            break;
        case Types.NUMERIC:
        case Types.DECIMAL:
            BigDecimal bd =
                NumberUtil.rescaleBigDecimal(
                    new BigDecimal(val),
                    paramMetaData.scale);
            checkRange(
                bd.unscaledValue(),
                (BigInteger) min,
                (BigInteger) max);
            datum.setLong(bd.unscaledValue().longValue());
            break;
        case Types.BIT:
        case Types.BOOLEAN:
            datum.setBoolean(val != 0);
            break;
        case Types.REAL:
            checkRange(
                val,
                min.doubleValue(),
                max.doubleValue());
            datum.setFloat((float) val);
            break;
        case Types.FLOAT:
        case Types.DOUBLE:
            checkRange(
                val,
                min.doubleValue(),
                max.doubleValue());
            datum.setDouble((double) val);
            break;
        case Types.VARCHAR:
        case Types.CHAR:
            if (isFloat) {
                setString(
                    paramMetaData.type == Types.CHAR,
                    datum,
                    Float.toString((float) val),
                    clazz);
            } else {
                setString(
                    paramMetaData.type == Types.CHAR,
                    datum,
                    Double.toString(val),
                    clazz);
            }
            break;
        default:
            throw newInvalidType(clazz);
        }
    }

    public void setBigDecimal(FennelTupleDatum datum, BigDecimal val)
    {
        if (val == null) {
            setNull(datum);
            return;
        }

        BigDecimal bd;
        switch (paramMetaData.type) {
        case Types.TINYINT:
            bd = NumberUtil.rescaleBigDecimal(val, 0);
            checkRange(
                bd.doubleValue(),
                min.doubleValue(),
                max.doubleValue());
            datum.setByte(bd.byteValue());
            break;
        case Types.SMALLINT:
            bd = NumberUtil.rescaleBigDecimal(val, 0);
            checkRange(
                bd.doubleValue(),
                min.doubleValue(),
                max.doubleValue());
            datum.setShort(bd.shortValue());
            break;
        case Types.INTEGER:
            bd = NumberUtil.rescaleBigDecimal(val, 0);
            checkRange(
                bd.doubleValue(),
                min.doubleValue(),
                max.doubleValue());
            datum.setInt(bd.intValue());
            break;
        case Types.BIGINT:
            bd = NumberUtil.rescaleBigDecimal(val, 0);
            checkRange(
                bd.doubleValue(),
                min.doubleValue(),
                max.doubleValue());
            datum.setLong(bd.longValue());
            break;
        case Types.NUMERIC:
        case Types.DECIMAL:
            bd = NumberUtil.rescaleBigDecimal(val, paramMetaData.scale);
            checkRange(
                bd.unscaledValue(),
                (BigInteger) min,
                (BigInteger) max);
            datum.setLong(bd.unscaledValue().longValue());
            break;
        case Types.BIT:
        case Types.BOOLEAN:
            datum.setBoolean(!val.equals(BigDecimal.valueOf(0)));
            break;
        case Types.REAL:
            checkRange(
                val.doubleValue(),
                min.doubleValue(),
                max.doubleValue());
            datum.setFloat(val.floatValue());
            break;
        case Types.FLOAT:
        case Types.DOUBLE:
            checkRange(
                val.doubleValue(),
                min.doubleValue(),
                max.doubleValue());
            datum.setDouble(val.doubleValue());
            break;
        case Types.VARCHAR:
        case Types.CHAR:
            setString(
                paramMetaData.type == Types.CHAR,
                datum,
                val.toString(),
                BigDecimal.class);
            break;
        default:
            throw newInvalidType(BigDecimal.class);
        }
    }

    private void setString(
        boolean pad,
        FennelTupleDatum datum,
        String val,
        Class clazz)
    {
        if (datum.getCapacity() >= val.length()) {
            if (pad && (datum.getCapacity() > val.length())) {
                // Use StringBuffer instead of StringBuilder for JDK 1.4
                // compatibility
                StringBuffer buf = new StringBuffer(datum.getCapacity());
                buf.append(val);
                for (int i = val.length(); i < datum.getCapacity(); i++) {
                    buf.append(' ');
                }
                val = buf.toString();
            }
            datum.setString(val);
        } else {
            throw newValueTooLong(val);
        }
    }

    public void setString(FennelTupleDatum datum, String val)
    {
        if (val == null) {
            setNull(datum);
            return;
        }

        switch (paramMetaData.type) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
            try {
                long n = Long.parseLong(val.trim());
                checkRange(
                    n,
                    min.longValue(),
                    max.longValue());
                datum.setLong(n);
            } catch (NumberFormatException e) {
                throw newInvalidFormat(val);
            }
            break;
        case Types.NUMERIC:
        case Types.DECIMAL:
            try {
                BigDecimal bd =
                    NumberUtil.rescaleBigDecimal(
                        new BigDecimal(val.trim()),
                        paramMetaData.scale);
                checkRange(
                    bd.unscaledValue(),
                    (BigInteger) min,
                    (BigInteger) max);
                datum.setLong(bd.unscaledValue().longValue());
            } catch (Throwable ex) {
                throw newInvalidFormat(val);
            }
            break;
        case Types.BIT:
        case Types.BOOLEAN:
            try {
                Boolean boolVal = ConversionUtil.toBoolean(val.trim());
                if (boolVal == null) {
                    setNull(datum);
                } else {
                    datum.setBoolean(boolVal.booleanValue());
                }
            } catch (Throwable ex) {
                // Convert string to number, return false if zero
                try {
                    double d = Double.parseDouble(val.trim());
                    datum.setBoolean(d != 0);
                } catch (NumberFormatException e) {
                    throw newInvalidFormat(val);
                }
            }
            break;
        case Types.REAL:
            try {
                float n = Float.parseFloat(val.trim());
                checkRange(
                    n,
                    min.doubleValue(),
                    max.doubleValue());
                datum.setFloat(n);
            } catch (NumberFormatException e) {
                throw newInvalidFormat(val);
            }
            break;
        case Types.FLOAT:
        case Types.DOUBLE:
            try {
                double n = Double.parseDouble(val.trim());
                checkRange(
                    n,
                    min.doubleValue(),
                    max.doubleValue());
                datum.setDouble(n);
            } catch (NumberFormatException e) {
                throw newInvalidFormat(val);
            }
            break;
        case Types.VARCHAR:
        case Types.CHAR:
            setString(
                paramMetaData.type == Types.CHAR,
                datum,
                val,
                val.getClass());
            break;
        case Types.DATE:
            try {
                datum.setLong(Date.valueOf(val.trim()).getTime());
            } catch (IllegalArgumentException e) {
                throw newInvalidFormat(val);
            }
            break;
        case Types.TIME:
            try {
                datum.setLong(Time.valueOf(val.trim()).getTime());
            } catch (IllegalArgumentException e) {
                throw newInvalidFormat(val);
            }
            break;
        case Types.TIMESTAMP:
            try {
                datum.setLong(Timestamp.valueOf(val.trim()).getTime());
            } catch (IllegalArgumentException e) {
                throw newInvalidFormat(val);
            }
            break;
        default:
            throw newInvalidType(val);
        }
    }

    public void setDate(FennelTupleDatum datum, ZonelessDate val)
    {
        if (val == null) {
            setNull(datum);
            return;
        }
        switch (paramMetaData.type) {
        case Types.CHAR:
        case Types.VARCHAR:
            setString(
                paramMetaData.type == Types.CHAR,
                datum,
                val.toString(),
                val.getClass());
            break;
        case Types.DATE:
        case Types.TIMESTAMP:
            datum.setLong(val.getTime());
            break;
        default:
            throw newInvalidType(val);
        }
    }

    public void setTime(FennelTupleDatum datum, ZonelessTime val)
    {
        if (val == null) {
            setNull(datum);
            return;
        }
        switch (paramMetaData.type) {
        case Types.CHAR:
        case Types.VARCHAR:
            setString(
                paramMetaData.type == Types.CHAR,
                datum,
                val.toString(),
                val.getClass());
            break;
        case Types.TIME:
            datum.setLong(val.getTime());
            break;
        default:
            throw newInvalidType(val);
        }
    }

    public void setTimestamp(FennelTupleDatum datum, ZonelessTimestamp val)
    {
        if (val == null) {
            setNull(datum);
            return;
        }
        switch (paramMetaData.type) {
        case Types.CHAR:
        case Types.VARCHAR:
            setString(
                paramMetaData.type == Types.CHAR,
                datum,
                val.toString(),
                val.getClass());
            break;
        case Types.TIME:
        case Types.DATE:
        case Types.TIMESTAMP:
            datum.setLong(val.getTime());
            break;
        default:
            throw newInvalidType(val);
        }
    }

    public void setBytes(FennelTupleDatum datum, byte [] val)
    {
        if (val == null) {
            setNull(datum);
            return;
        }
        switch (paramMetaData.type) {
        case Types.BINARY:
        case Types.VARBINARY:
            if (val.length > datum.getCapacity()) {
                throw newValueTooLong(val);
            }
            datum.setBytes(val);
            break;
        default:
            throw newInvalidType(val);
        }
    }

    public void setObject(FennelTupleDatum datum, Object val)
    {
        if (val == null) {
            setNull(datum);
            return;
        }

        if (val instanceof String) {
            setString(datum, (String) val);
        } else if (val instanceof Boolean) {
            setBoolean(
                datum,
                ((Boolean) val).booleanValue());
        } else if (val instanceof BigDecimal) {
            setBigDecimal(datum, (BigDecimal) val);
        } else if (val instanceof Number) {
            Number n = (Number) val;
            if (val instanceof Float) {
                setDouble(
                    datum,
                    n.doubleValue(),
                    false);
            } else if (val instanceof Double) {
                setDouble(
                    datum,
                    n.doubleValue(),
                    true);
            } else if (
                (val instanceof Byte)
                || (val instanceof Short)
                || (val instanceof Integer)
                || (val instanceof Long))
            {
                setLong(
                    datum,
                    n.longValue(),
                    val.getClass());
            } else {
                setBigDecimal(
                    datum,
                    NumberUtil.toBigDecimal(n));
            }
        } else if (val instanceof ZonelessTime) {
            setTime(datum, (ZonelessTime) val);
        } else if (val instanceof ZonelessDate) {
            setDate(datum, (ZonelessDate) val);
        } else if (val instanceof ZonelessTimestamp) {
            setTimestamp(datum, (ZonelessTimestamp) val);
        } else if (val instanceof byte []) {
            setBytes(datum, (byte []) val);
        } else {
            throw newInvalidType(val);
        }
    }
}

// End FarragoJdbcFennelTupleParamDef.java
