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

import net.sf.farrago.fennel.tuple.FennelTupleDatum;

import java.sql.*;
import java.math.BigInteger;
import java.math.BigDecimal;

import org.eigenbase.util14.NumberUtil;
import org.eigenbase.util14.ConversionUtil;

/**
 * FarragoJdbcFennelTupleParamDef represents a parameter associated with a
 * FennelTupleDatum.  It handles data converstions to the target type.
 *
 * @author Angel Chang
 * @version $Id$
 * @since March 3, 2006
 */
public class FarragoJdbcFennelTupleParamDef extends FarragoJdbcParamDef {

    protected Number min;
    protected Number max;

    public FarragoJdbcFennelTupleParamDef(
        String paramName,
        FarragoParamFieldMetaData param)
    {
        super(paramName, param);

        switch (paramMetaData.type) {
            case Types.TINYINT:
                min = Byte.valueOf(Byte.MIN_VALUE);
                max = Byte.valueOf(Byte.MAX_VALUE);
                break;
            case Types.SMALLINT:
                min = Short.valueOf(Short.MIN_VALUE);
                max = Short.valueOf(Short.MAX_VALUE);
                break;
            case Types.INTEGER:
                min = Integer.valueOf(Integer.MIN_VALUE);
                max = Integer.valueOf(Integer.MAX_VALUE);
                break;
            case Types.BIGINT:
                min = Long.valueOf(Long.MIN_VALUE);
                max = Long.valueOf(Long.MAX_VALUE);
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                min = NumberUtil.getMinUnscaled(paramMetaData.precision);
                max = NumberUtil.getMaxUnscaled(paramMetaData.precision);
                break;
            case Types.BIT:
            case Types.BOOLEAN:
                min = Integer.valueOf(0);
                max = Integer.valueOf(1);
                break;
            case Types.REAL:
                min = Float.valueOf(-Float.MAX_VALUE);
                max = Float.valueOf(Float.MAX_VALUE);
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
                min = Double.valueOf(-Double.MAX_VALUE);
                max = Double.valueOf(Double.MAX_VALUE);
                break;
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
                datum.setLong(b? 1: 0);
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                datum.setLong(b?
                    BigInteger.TEN.pow(paramMetaData.scale).longValue(): 0);
                break;
            case Types.BIT:
            case Types.BOOLEAN:
                datum.setBoolean(b);
                break;
            case Types.REAL:
                datum.setFloat(b? 1: 0);
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
                datum.setDouble(b? 1: 0);
                break;
            case Types.VARCHAR:
            case Types.CHAR:
                setString(datum, b? "TRUE":"FALSE", Boolean.class);
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
                checkRange(val, min.longValue(), max.longValue());
                datum.setLong((long) val);
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                BigDecimal bd = NumberUtil.rescaleBigDecimal(
                    BigDecimal.valueOf(val), paramMetaData.scale);
                checkRange(bd.unscaledValue(), (BigInteger) min, (BigInteger) max);
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
                setString(datum, Long.toString(val), clazz);
                break;
            default:
                throw newInvalidType(clazz);
        }
    }

    public void setFloat(FennelTupleDatum datum, float val)
    {
        setDouble(datum, (double) val, Float.class);
    }


    public void setDouble(FennelTupleDatum datum, double val)
    {
        setDouble(datum, (double) val, Double.class);
    }

    private void setDouble(FennelTupleDatum datum, double val, Class clazz)
    {
        switch (paramMetaData.type) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                long n = NumberUtil.round(val);
                checkRange(n, min.longValue(), max.longValue());
                datum.setLong(n);
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                BigDecimal bd = NumberUtil.rescaleBigDecimal(
                    BigDecimal.valueOf(val), paramMetaData.scale);
                checkRange(bd.unscaledValue(), (BigInteger) min, (BigInteger) max);
                datum.setLong(bd.unscaledValue().longValue());
                break;
            case Types.BIT:
            case Types.BOOLEAN:
                datum.setBoolean(val != 0);
                break;
            case Types.REAL:
                checkRange(val, min.doubleValue(), max.doubleValue());
                datum.setFloat((float) val);
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
                checkRange(val, min.doubleValue(), max.doubleValue());
                datum.setDouble((double) val);
                break;
            case Types.VARCHAR:
            case Types.CHAR:
                setString(datum, Double.toString(val), clazz);
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
                checkRange(bd.doubleValue(), min.doubleValue(), max.doubleValue());
                datum.setByte(bd.byteValue());
                break;
            case Types.SMALLINT:
                bd = NumberUtil.rescaleBigDecimal(val, 0);
                checkRange(bd.doubleValue(), min.doubleValue(), max.doubleValue());
                datum.setShort(bd.shortValue());
                break;
            case Types.INTEGER:
                bd = NumberUtil.rescaleBigDecimal(val, 0);
                checkRange(bd.doubleValue(), min.doubleValue(), max.doubleValue());
                datum.setInt(bd.intValue());
                break;
            case Types.BIGINT:
                bd = NumberUtil.rescaleBigDecimal(val, 0);
                checkRange(bd.doubleValue(), min.doubleValue(), max.doubleValue());
                datum.setLong(bd.longValue());
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                bd = NumberUtil.rescaleBigDecimal(val, paramMetaData.scale);
                checkRange(bd.unscaledValue(), (BigInteger) min, (BigInteger) max);
                datum.setLong(bd.unscaledValue().longValue());
                break;
            case Types.BIT:
            case Types.BOOLEAN:
                datum.setBoolean(!val.equals(BigDecimal.ZERO));
                break;
            case Types.REAL:
                checkRange(val.doubleValue(), min.doubleValue(), max.doubleValue());
                datum.setFloat(val.floatValue());
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
                checkRange(val.doubleValue(), min.doubleValue(), max.doubleValue());
                datum.setDouble(val.doubleValue());
                break;
            case Types.VARCHAR:
            case Types.CHAR:
                setString(datum, val.toString(),  BigDecimal.class);
                break;
            default:
                throw newInvalidType(BigDecimal.class);
        }
    }

    private void setString(FennelTupleDatum datum, String val, Class clazz)
    {
        if (datum.getCapacity() >= val.length()) {
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
                    checkRange(n, min.longValue(), max.longValue());
                    datum.setLong(n);
                } catch (NumberFormatException e) {
                    throw newInvalidFormat(val);
                }
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                try {
                    BigDecimal bd = NumberUtil.rescaleBigDecimal(
                        new BigDecimal(val.trim()), paramMetaData.scale);
                    checkRange(bd.unscaledValue(), (BigInteger) min, (BigInteger) max);
                    datum.setLong(bd.unscaledValue().longValue());
                } catch (Throwable ex) {
                    throw newInvalidFormat(val);
                }
                break;
            case Types.BIT:
            case Types.BOOLEAN:
                try {
                    Boolean boolVal = ConversionUtil.toBoolean(val);
                    if (boolVal == null) {
                        setNull(datum);
                    } else {
                        datum.setBoolean(boolVal.booleanValue());
                    }
                } catch (Throwable ex) {
                    throw newInvalidFormat(val);
                }
                break;
            case Types.REAL:
                try {
                    float n = Float.parseFloat(val.trim());
                    checkRange(n, min.doubleValue(), max.doubleValue());
                    datum.setFloat(n);
                } catch (NumberFormatException e) {
                    throw newInvalidFormat(val);
                }
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
                try {
                    double n = Double.parseDouble(val.trim());
                    checkRange(n, min.doubleValue(), max.doubleValue());
                    datum.setDouble(n);
                } catch (NumberFormatException e) {
                    throw newInvalidFormat(val);
                }
                break;
            case Types.VARCHAR:
            case Types.CHAR:
                setString(datum, val, val.getClass());
                break;
            default:
                throw newInvalidType(val);
        }
    }

    public void setDate(FennelTupleDatum datum, Date val)
    {
        if (val == null) {
            setNull(datum);
            return;
        }
        switch (paramMetaData.type) {
            case Types.CHAR:
            case Types.VARCHAR:
                setString(datum, val.toString(), val.getClass());
                break;
            case Types.DATE:
            case Types.TIMESTAMP:
                datum.setLong(val.getTime());
                break;
            default:
                throw newInvalidType(val);
        }
    }

    public void setTime(FennelTupleDatum datum, Time val)
    {
        if (val == null) {
            setNull(datum);
            return;
        }
        switch (paramMetaData.type) {
            case Types.CHAR:
            case Types.VARCHAR:
                setString(datum, val.toString(), val.getClass());
                break;
            case Types.TIME:
            case Types.TIMESTAMP:
                datum.setLong(val.getTime());
                break;
            default:
                throw newInvalidType(val);
        }
    }

    public void setTimestamp(FennelTupleDatum datum, Timestamp val)
    {
        if (val == null) {
            setNull(datum);
            return;
        }
        switch (paramMetaData.type) {
            case Types.CHAR:
            case Types.VARCHAR:
                setString(datum, val.toString(), val.getClass());
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

    public void setBytes(FennelTupleDatum datum, byte val[])
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
            setBoolean(datum, ((Boolean) val).booleanValue());
        } else if (val instanceof BigDecimal) {
            setBigDecimal(datum, (BigDecimal) val);
        } else if (val instanceof Number) {
            Number n = (Number) val;
            if (val instanceof Float || val instanceof Double) {
                setDouble(datum, n.doubleValue(), val.getClass());
            } else if (val instanceof Byte || val instanceof Short ||
                val instanceof Integer || val instanceof Long) {
                setLong(datum, n.longValue(), val.getClass());
            } else {
                setBigDecimal(datum, NumberUtil.toBigDecimal(n));
            }
        } else if (val instanceof Time) {
            setTime(datum, (Time) val);
        } else if (val instanceof Date) {
            setDate(datum, (Date) val);
        } else if (val instanceof Timestamp) {
            setTimestamp(datum, (Timestamp) val);
        } else if (val instanceof byte[]) {
            setBytes(datum, (byte[]) val);
        } else {
            throw newInvalidType(val);
        }
    }
}

// End FarragoJdbcFennelTupleParamDef.java