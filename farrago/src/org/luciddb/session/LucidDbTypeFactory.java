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
package org.luciddb.session;

import net.sf.farrago.catalog.*;
import net.sf.farrago.type.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;


/**
 * LucidDbTypeFactory overrides {@link FarragoTypeFactoryImpl} with
 * LucidDB-specific type derivation rules.
 *
 * @author John Pham
 * @version $Id$
 */
public class LucidDbTypeFactory
    extends FarragoTypeFactoryImpl
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Cap applied to the scale of a decimal multiplication product, if the
     * product's ideal precision is too high to be supported by LucidDb.
     */
    public static final int DECIMAL_PRODUCT_SCALE_CAP = 6;

    /**
     * Cap applied to the scale of a decimal division quotient.
     */
    public static final int DECIMAL_QUOTIENT_SCALE_CAP = 6;

    //~ Constructors -----------------------------------------------------------

    public LucidDbTypeFactory(FarragoRepos repos)
    {
        super(repos);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Similar to {@link RelDataTypeFactoryImpl#createDecimalProduct} but caps
     * the maximum scale at {@link #DECIMAL_PRODUCT_SCALE_CAP}.
     */
    public RelDataType createDecimalProduct(
        RelDataType type1,
        RelDataType type2)
    {
        if (SqlTypeUtil.isExactNumeric(type1)
            && SqlTypeUtil.isExactNumeric(type2))
        {
            if (SqlTypeUtil.isDecimal(type1)
                || SqlTypeUtil.isDecimal(type2))
            {
                int p1 = type1.getPrecision();
                int p2 = type2.getPrecision();
                int s1 = type1.getScale();
                int s2 = type2.getScale();

                int scale = s1 + s2;
                scale = Math.min(scale, SqlTypeName.MAX_NUMERIC_SCALE);
                int precision = p1 + p2;

                // if precision is too great and we have to make a choice
                // between integer digits and scale, then favor integer
                // digits once certain scale is reached
                if ((precision > SqlTypeName.MAX_NUMERIC_PRECISION)
                    && (scale > DECIMAL_PRODUCT_SCALE_CAP))
                {
                    int pDiff = precision - SqlTypeName.MAX_NUMERIC_PRECISION;
                    int sDiff = scale - DECIMAL_PRODUCT_SCALE_CAP;
                    int adjustment = Math.min(pDiff, sDiff);
                    scale -= adjustment;
                }
                precision =
                    Math.min(
                        precision,
                        SqlTypeName.MAX_NUMERIC_PRECISION);

                RelDataType ret =
                    createSqlType(
                        SqlTypeName.DECIMAL,
                        precision,
                        scale);

                return ret;
            }
        }

        return null;
    }

    // implement RelDataTypeFactory
    public boolean useDoubleMultiplication(
        RelDataType type1,
        RelDataType type2)
    {
        assert (createDecimalProduct(type1, type2) != null);
        int p1 = type1.getPrecision();
        int p2 = type2.getPrecision();

        // use double multiplication whenever the result might overflow
        return (p1 + p2) >= SqlTypeName.MAX_NUMERIC_PRECISION;
    }

    /**
     * Similar to {@link RelDataTypeFactoryImpl#createDecimalQuotient} but caps
     * the maximum scale at {@link #DECIMAL_QUOTIENT_SCALE_CAP}.
     */
    public RelDataType createDecimalQuotient(
        RelDataType type1,
        RelDataType type2)
    {
        if (SqlTypeUtil.isExactNumeric(type1)
            && SqlTypeUtil.isExactNumeric(type2))
        {
            int p1 = type1.getPrecision();
            int p2 = type2.getPrecision();
            int s1 = type1.getScale();
            int s2 = type2.getScale();

            int dout =
                Math.min(
                    p1 - s1 + s2,
                    SqlTypeName.MAX_NUMERIC_PRECISION);

            int scale = Math.max(6, s1 + p2 + 1);

            // LucidDb preserves the scale, but caps it, in order to
            // preserve the integral part of the result.
            scale = Math.min(scale, DECIMAL_QUOTIENT_SCALE_CAP);
            dout = Math.min(
                dout,
                SqlTypeName.MAX_NUMERIC_PRECISION - scale);

            int precision = dout + scale;
            assert (precision <= SqlTypeName.MAX_NUMERIC_PRECISION);
            assert (precision > 0);

            RelDataType ret;
            ret =
                createSqlType(
                    SqlTypeName.DECIMAL,
                    precision,
                    scale);

            return ret;
        }

        return null;
    }

    // override SqlTypeFactoryImpl by requesting pragmatic behavior
    // instead of strict SQL:2003 behavior
    protected boolean shouldRaggedFixedLengthValueUnionBeVariable()
    {
        return true;
    }
}

// End LucidDbTypeFactory.java
