/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2004-2004 John V. Sichi.
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
package net.sf.farrago.ddl;

import org.eigenbase.util.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fem.med.*;

import java.io.*;
import java.nio.charset.*;
import java.sql.*;
import java.util.*;
import java.util.logging.*;

/**
 * DdlHandler is an abstract base for classes which provide implementations for
 * the actions taken by DdlValidator on individual objects.  See {@link
 * FarragoSessionDdlHandler} for an explanation.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class DdlHandler
{
    protected static final Logger tracer = FarragoTrace.getDdlValidatorTracer();
    
    protected final DdlValidator validator;

    protected final FarragoRepos repos;
    
    public DdlHandler(DdlValidator validator)
    {
        this.validator = validator;
        repos = validator.getRepos();
    }

    public DdlValidator getValidator()
    {
        return validator;
    }
    
    public void validateBaseColumnSet(FemBaseColumnSet columnSet)
    {
        validator.validateUniqueNames(
            columnSet,
            columnSet.getFeature(),
            false);

        Iterator iter = columnSet.getFeature().iterator();
        while (iter.hasNext()) {
            FemStoredColumn column = (FemStoredColumn) iter.next();
            validateColumnImpl(column);
        }

        // REVIEW jvs 21-Jan-2005:  something fishy here...

        // Foreign tables should not support constraint definitions.  Eventually
        // we may want to allow this as a hint to the optimizer, but it's not
        // standard so for now we should prevent it.
        Iterator constraintIter = columnSet.getOwnedElement().iterator();
        while (constraintIter.hasNext()) {
            Object obj = constraintIter.next();
            if (!(obj instanceof CwmUniqueConstraint)) {
                continue;
            }
            throw validator.res.newValidatorNoConstraintAllowed(
                repos.getLocalizedObjectName(columnSet));
        }
    }

    public void validateColumnImpl(FemAbstractColumn column)
    {
        int ordinal = column.getOwner().getFeature().indexOf(column);
        assert (ordinal != -1);
        column.setOrdinal(ordinal);

        validateTypedElement(column);
    }
    
    public void validateTypedElement(FemSqltypedElement element)
    {
        if (element.getInitialValue() == null) {
            CwmExpression nullExpression = repos.newCwmExpression();
            nullExpression.setLanguage("SQL");
            nullExpression.setBody("NULL");
            element.setInitialValue(nullExpression);
        }

        // if NOT NULL not specified, default to nullable
        if (element.getIsNullable() == null) {
            element.setIsNullable(NullableTypeEnum.COLUMN_NULLABLE);
        }

        SqlTypeName typeName = SqlTypeName.get(
            element.getType().getName());

        // NOTE: parser only generates precision, but CWM discriminates
        // precision from length, so we take care of it below
        Integer precision = element.getPrecision();
        if (precision == null) {
            precision = element.getLength();
        }

        // TODO:  break this method up
        // first, validate presence of modifiers
        if ((typeName != null) && typeName.allowsPrec()) {
            if (precision == null) {
                int p = typeName.getDefaultPrecision();
                if (p != -1) {
                    precision = new Integer(p);
                }
            }
            if ((precision == null) && !typeName.allowsNoPrecNoScale()) {
                throw validator.newPositionalError(
                    element,
                    validator.res.newValidatorPrecRequired(
                        repos.getLocalizedObjectName(element.getType()),
                        repos.getLocalizedObjectName(element)));
            }
        } else {
            if (precision != null) {
                throw validator.newPositionalError(
                    element,
                    validator.res.newValidatorPrecUnexpected(
                        repos.getLocalizedObjectName(element.getType()),
                        repos.getLocalizedObjectName(element)));
            }
        }
        if ((typeName != null) && typeName.allowsScale()) {
            // assume scale is always optional
        } else {
            if (element.getScale() != null) {
                throw validator.newPositionalError(
                    element,
                    validator.res.newValidatorScaleUnexpected(
                        repos.getLocalizedObjectName(element.getType()),
                        repos.getLocalizedObjectName(element)));
            }
        }
        SqlTypeFamily typeFamily = null;
        if (typeName != null) {
            typeFamily = SqlTypeFamily.getFamilyForSqlType(typeName);
        }
        if ((typeFamily == SqlTypeFamily.Character)
            || (typeFamily == SqlTypeFamily.Binary))
        {
            // convert precision to length
            if (element.getLength() == null) {
                element.setLength(element.getPrecision());
                element.setPrecision(null);
            }
        }
        if (typeFamily == SqlTypeFamily.Character) {
            // TODO jvs 18-April-2004:  Should be inheriting these defaults
            // from schema/catalog.
            if (JmiUtil.isBlank(element.getCharacterSetName())) {
                // NOTE: don't leave character set name implicit, since if the
                // default ever changed, that would invalidate existing data
                element.setCharacterSetName(
                    repos.getDefaultCharsetName());
            } else {
                if (!Charset.isSupported(element.getCharacterSetName())) {
                    throw validator.newPositionalError(
                        element,
                        validator.res.newValidatorCharsetUnsupported(
                            element.getCharacterSetName(),
                            repos.getLocalizedObjectName(element)));
                }
            }
            Charset charSet = Charset.forName(element.getCharacterSetName());
            if (charSet.newEncoder().maxBytesPerChar() > 1) {
                // TODO:  implement multi-byte character sets
                throw Util.needToImplement(charSet);
            }
        } else {
            if (!JmiUtil.isBlank(element.getCharacterSetName())) {
                throw validator.newPositionalError(
                    element,
                    validator.res.newValidatorCharsetUnexpected(
                        repos.getLocalizedObjectName(element.getType()),
                        repos.getLocalizedObjectName(element)));
            }
        }

        // now, enforce type-defined limits
        CwmSqldataType cwmType = validator.getStmtValidator().findSqldataType(
            element.getType().getName());

        if (cwmType instanceof CwmSqlsimpleType) {
            CwmSqlsimpleType simpleType = (CwmSqlsimpleType) cwmType;

            if (element.getLength() != null) {
                Integer maximum = simpleType.getCharacterMaximumLength();
                assert (maximum != null);
                if (element.getLength().intValue() > maximum.intValue()) {
                    throw validator.newPositionalError(
                        element,
                        validator.res.newValidatorLengthExceeded(
                            element.getLength(),
                            maximum,
                            repos.getLocalizedObjectName(element)));
                }
            }
            if (element.getPrecision() != null) {
                Integer maximum = simpleType.getNumericPrecision();
                if (maximum == null) {
                    maximum = simpleType.getDateTimePrecision();
                }
                assert (maximum != null);
                if (element.getPrecision().intValue() > maximum.intValue()) {
                    throw validator.newPositionalError(
                        element,
                        validator.res.newValidatorPrecisionExceeded(
                            element.getPrecision(),
                            maximum,
                            repos.getLocalizedObjectName(element)));
                }
            }
            if (element.getScale() != null) {
                Integer maximum = simpleType.getNumericScale();
                assert (maximum != null);
                if (element.getScale().intValue() > maximum.intValue()) {
                    throw validator.newPositionalError(
                        element,
                        validator.res.newValidatorScaleExceeded(
                            element.getScale(),
                            maximum,
                            repos.getLocalizedObjectName(element)));
                }
            }
        } else if (cwmType instanceof FemSqlcollectionType) {
            FemSqlcollectionType collectionType =
                (FemSqlcollectionType) element.getType();
            FemSqltypedElement componentType =
                collectionType.getComponentType();
            validateTypedElement(componentType);
        } else {
            Util.permAssert(false,
                "only simple and collection types are supported");
        }

        // REVIEW jvs 18-April-2004: I had to put these in because CWM
        // declares them as mandatory.  This is stupid, since CWM also says
        // these fields are inapplicable for non-character types.
        if (element.getCollationName() == null) {
            element.setCollationName("");
        }

        if (element.getCharacterSetName() == null) {
            element.setCharacterSetName("");
        }
    }

    /**
     * Initializes a CwmColumn definition based on a RelDataTypeField.
     * If the column has no name, the name is initialized from the field
     * name; otherwise, the existing name is left unmodified.
     *
     * @param field input field
     *
     * @param column on input, contains unintialized CwmColumn instance;
     * on return, this has been initialized (but not validated)
     */
    public void convertFieldToCwmColumn(
        RelDataTypeField field,
        CwmColumn column)
    {
        RelDataType type = field.getType();
        if (column.getName() == null) {
            column.setName(field.getName());
        }
        SqlTypeName typeName = type.getSqlTypeName();
        String lookupName;
        if (typeName == null) {
            // TODO jvs 15-Dec-2005:  UDT etc
            lookupName = type.toString();
        } else {
            lookupName = typeName.getName();
        }
        CwmSqldataType cwmType = 
            validator.getStmtValidator().findSqldataType(lookupName);
        column.setType(cwmType);
        if (typeName != null) {
            if (typeName.allowsPrec()) {
                column.setPrecision(new Integer(type.getPrecision()));
                if (typeName.allowsScale()) {
                    column.setScale(new Integer(type.getScale()));
                }
            }
        }
        if (type.isNullable()) {
            column.setIsNullable(NullableTypeEnum.COLUMN_NULLABLE);
        } else {
            column.setIsNullable(NullableTypeEnum.COLUMN_NO_NULLS);
        }
    }
}

// End DdlHandler.java
