/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.cwm.relational;

import net.sf.farrago.catalog.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.netbeans.mdr.handlers.*;
import org.netbeans.mdr.storagemodel.*;

import org.eigenbase.reltype.*;
import org.eigenbase.util.*;

import java.sql.*;
import java.nio.charset.*;

/**
 * CwmColumnImpl is a custom implementation for CWM CwmColumn.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class CwmColumnImpl extends InstanceHandler
    implements CwmColumn,
        DdlValidatedElement
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new CwmColumnImpl object.
     *
     * @param storable .
     */
    protected CwmColumnImpl(StorableObject storable)
    {
        super(storable);
    }

    //~ Methods ---------------------------------------------------------------

    // implement DdlValidatedElement
    public void validateDefinition(DdlValidator validator,boolean creation)
    {
        // let the containing table handle this by calling our
        // validateDefinitionImpl, since sometimes it has extra work to do
        // first
    }

    public void validateDefinitionImpl(DdlValidator validator)
    {
        validateCommon(validator,this);

        String defaultExpression = getInitialValue().getBody();
        if (!defaultExpression.equalsIgnoreCase("NULL")) {
            FarragoSession session = validator.newReentrantSession();
            try {
                validateDefaultClause(validator,session,defaultExpression);
            } catch (Throwable ex) {
                throw validator.res.newValidatorBadDefaultClause(
                    getName(),
                    validator.getParserPosString(this),
                    ex);
                
            } finally {
                validator.releaseReentrantSession(session);
            }
        }
    }

    // implement DdlValidatedElement
    public void validateDeletion(DdlValidator validator,boolean truncation)
    {
        assert (!truncation);

        // REVIEW:  any dependent objs?
    }

    private void validateDefaultClause(
        DdlValidator validator,
        FarragoSession session,
        String defaultExpression)
    {
        String sql = "VALUES(" + defaultExpression + ")";
        FarragoSessionStmtContext stmtContext = session.newStmtContext();
        stmtContext.prepare(sql,false);
        RelDataType rowType = stmtContext.getPreparedRowType();
        assert(rowType.getFieldCount() == 1);

        if (stmtContext.getPreparedParamType().getFieldCount() > 0) {
            throw validator.res.newValidatorBadDefaultParam(
                getName(),
                validator.getParserPosString(this));
        }

        // SQL standard is very picky about what can go in a DEFAULT clause

        FarragoAtomicType sourceType = (FarragoAtomicType)
            rowType.getFields()[0].getType();
        FarragoTypeFamily sourceTypeFamily = sourceType.getFamily();
        
        FarragoType targetType =
            validator.getTypeFactory().createColumnType(this,true);
        FarragoAtomicType atomicType = (FarragoAtomicType) targetType;
        FarragoTypeFamily targetTypeFamily = atomicType.getFamily();
        
        if (sourceTypeFamily != targetTypeFamily) {
            throw validator.res.newValidatorBadDefaultType(
                getName(),
                targetTypeFamily.getName(),
                sourceTypeFamily.getName(),
                validator.getParserPosString(this));
        }

        // TODO:  additional rules from standard, like no truncation allowed.
        // Maybe just execute SELECT with and without cast to target type and
        // make sure the same value comes back.
    }

    public static void validateCommon(
        DdlValidator validator,
        CwmColumn column)
    {
        if (column instanceof FemAbstractColumn) {
            // REVIEW jvs 5-April-2004:  This is kind of sneaky.
            // Need to reorg all this column stuff.
            FemAbstractColumn femColumn = (FemAbstractColumn) column;
            int ordinal = column.getOwner().getFeature().indexOf(column);
            assert(ordinal != -1);
            femColumn.setOrdinal(ordinal);
        }

        if (column.getInitialValue() == null) {
            CwmExpression nullExpression =
                validator.getCatalog().newCwmExpression();
            nullExpression.setLanguage("SQL");
            nullExpression.setBody("NULL");
            column.setInitialValue(nullExpression);
        }

        // if NOT NULL not specified, default to nullable
        if (column.getIsNullable() == null) {
            column.setIsNullable(NullableTypeEnum.COLUMN_NULLABLE);
        }

        FarragoAtomicType type = (FarragoAtomicType)
            validator.getTypeFactory().createColumnType(
                column,false);
            
        // NOTE: parser only generates precision, but CWM discriminates
        // precision from length, so we take care of it below
        Integer precision = column.getPrecision();
        if (precision == null) {
            precision = column.getLength();
        }

        // TODO:  break this method up
        // first, validate presence of modifiers
        if (type.takesPrecision()) {
            if (precision == null) {
                precision = type.getDefaultPrecision();
            }
            if (precision == null) {
                throw validator.res.newValidatorPrecRequired(
                    getLocalizedTypeName(validator,column),
                    getLocalizedName(validator,column),
                    validator.getParserPosString(column));
            }
        } else {
            if (precision != null) {
                throw validator.res.newValidatorPrecUnexpected(
                    getLocalizedTypeName(validator,column),
                    getLocalizedName(validator,column),
                    validator.getParserPosString(column));
            }
        }
        if (type.takesScale()) {
            // assume scale is always optional
        } else {
            if (column.getScale() != null) {
                throw validator.res.newValidatorScaleUnexpected(
                    getLocalizedTypeName(validator,column),
                    getLocalizedName(validator,column),
                    validator.getParserPosString(column));
            }
        }
        if (type.isString()) {
            // convert precision to length
            if (column.getLength() == null) {
                column.setLength(column.getPrecision());
                column.setPrecision(null);
            }
        }
        if (type.getFamily() == FarragoTypeFamily.CHARACTER) {
            // TODO jvs 18-April-2004:  Should be inheriting these defaults
            // from schema/catalog.
            
            if (JmiUtil.isBlank(column.getCharacterSetName())) {
                // NOTE: don't leave character set name implicit, since if the
                // default ever changed, that would invalidate existing data
                column.setCharacterSetName(
                    validator.getCatalog().getDefaultCharsetName());
            } else {
                if (!Charset.isSupported(column.getCharacterSetName())) {
                    throw validator.res.newValidatorCharsetUnsupported(
                        column.getCharacterSetName(),
                        getLocalizedName(validator,column),
                        validator.getParserPosString(column));
                }
            }
            Charset charSet = Charset.forName(column.getCharacterSetName());
            if (charSet.newEncoder().maxBytesPerChar() > 1) {
                // TODO:  implement multi-byte character sets
                throw Util.needToImplement(charSet);
            }
        } else {
            if (!JmiUtil.isBlank(column.getCharacterSetName())) {
                throw validator.res.newValidatorCharsetUnexpected(
                    getLocalizedTypeName(validator,column),
                    getLocalizedName(validator,column),
                    validator.getParserPosString(column));
            }
        }

        // now, enforce type-defined limits
        CwmSqlsimpleType simpleType = type.getSimpleType();
        if (column.getLength() != null) {
            Integer maximum = simpleType.getCharacterMaximumLength();
            assert (maximum != null);
            if (column.getLength().intValue() > maximum.intValue()) {
                throw validator.res.newValidatorLengthExceeded(
                    column.getLength(),
                    maximum,
                    getLocalizedName(validator,column),
                    validator.getParserPosString(column));
            }
        }
        if (column.getPrecision() != null) {
            Integer maximum = simpleType.getNumericPrecision();
            if (maximum == null) {
                maximum = simpleType.getDateTimePrecision();
            }
            assert (maximum != null);
            if (column.getPrecision().intValue() > maximum.intValue()) {
                throw validator.res.newValidatorPrecisionExceeded(
                    column.getPrecision(),
                    maximum,
                    getLocalizedName(validator,column),
                    validator.getParserPosString(column));
            }
        }
        if (column.getScale() != null) {
            Integer maximum = simpleType.getNumericScale();
            assert (maximum != null);
            if (column.getScale().intValue() > maximum.intValue()) {
                throw validator.res.newValidatorScaleExceeded(
                    column.getScale(),
                    maximum,
                    getLocalizedName(validator,column),
                    validator.getParserPosString(column));
            }
        }

        // REVIEW jvs 18-April-2004: I had to put these in because CWM declares
        // them as mandatory.  This is stupid, since CWM also says these fields
        // are inapplicable for non-character columns.
        
        if (column.getCollationName() == null) {
            column.setCollationName("");
        }
        
        if (column.getCharacterSetName() == null) {
            column.setCharacterSetName("");
        }
        
    }

    private static String getLocalizedName(
        DdlValidator validator,CwmColumn column)
    {
        return validator.getCatalog().getLocalizedObjectName(
            null,
            column.getName(),
            column.refClass());
    }

    private static String getLocalizedTypeName(
        DdlValidator validator,CwmColumn column)
    {
        return validator.getCatalog().getLocalizedObjectName(
            column.getType(),
            column.getType().refClass());
    }
}

// End CwmColumnImpl.java
