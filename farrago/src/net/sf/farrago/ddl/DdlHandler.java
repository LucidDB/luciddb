/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
package net.sf.farrago.ddl;

import org.eigenbase.util.*;
import org.eigenbase.resource.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
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

    public void validateAttributeSet(CwmClass cwmClass)
    {
        List structuralFeatures =
            FarragoCatalogUtil.getStructuralFeatures(cwmClass);
        validator.validateUniqueNames(
            cwmClass,
            structuralFeatures,
            false);

        Iterator iter = structuralFeatures.iterator();
        while (iter.hasNext()) {
            FemAbstractAttribute attribute = (FemAbstractAttribute) iter.next();
            validateAttribute(attribute);
        }
    }
    
    public void validateBaseColumnSet(FemBaseColumnSet columnSet)
    {
        validateAttributeSet(columnSet);

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

    public void validateAttribute(FemAbstractAttribute attribute)
    {
        // REVIEW jvs 26-Feb-2005:  This relies on the fact that
        // attributes always come before operations.  We'll need to
        // take this into account in the implementation of ALTER TYPE.
        int ordinal = attribute.getOwner().getFeature().indexOf(attribute);
        assert (ordinal != -1);
        attribute.setOrdinal(ordinal);

        if (attribute.getInitialValue() == null) {
            CwmExpression nullExpression = repos.newCwmExpression();
            nullExpression.setLanguage("SQL");
            nullExpression.setBody("NULL");
            attribute.setInitialValue(nullExpression);
        }

        // if NOT NULL not specified, default to nullable
        if (attribute.getIsNullable() == null) {
            attribute.setIsNullable(NullableTypeEnum.COLUMN_NULLABLE);
        }
        
        validateTypedElement(attribute);

        String defaultExpression = attribute.getInitialValue().getBody();
        if (!defaultExpression.equalsIgnoreCase("NULL")) {
            FarragoSession session = validator.newReentrantSession();
            try {
                validateDefaultClause(attribute, session, defaultExpression);
            } catch (Throwable ex) {
                throw validator.newPositionalError(
                    attribute,
                    validator.res.newValidatorBadDefaultClause(
                        repos.getLocalizedObjectName(attribute), 
                        ex));
            } finally {
                validator.releaseReentrantSession(session);
            }
        }
    }

    private void convertSqlToCatalogType(
        SqlDataTypeSpec dataType,
        FemSqltypedElement element)
    {
        CwmSqldataType type =
            validator.getStmtValidator().findSqldataType(
                dataType.getTypeName());
        
        element.setType(type);
        if (dataType.getPrecision() > 0) {
            element.setPrecision(new Integer(dataType.getPrecision()));
        }
        if (dataType.getScale() > 0) {
            element.setScale(new Integer(dataType.getScale()));
        }
        if (dataType.getCharSetName() != null) {
            element.setCharacterSetName(dataType.getCharSetName());
        }
    }
    
    public void validateTypedElement(FemAbstractTypedElement abstractElement)
    {
        FemSqltypedElement element = FarragoCatalogUtil.toFemSqltypedElement(
            abstractElement);

        SqlDataTypeSpec dataType = (SqlDataTypeSpec)
            validator.getSqlDefinition(abstractElement);

        if (dataType != null) {
            convertSqlToCatalogType(dataType, element);
        }
        
        CwmSqldataType type = (CwmSqldataType) element.getType();
        SqlTypeName typeName = SqlTypeName.get(type.getName());

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
                    abstractElement,
                    validator.res.newValidatorPrecRequired(
                        repos.getLocalizedObjectName(type),
                        repos.getLocalizedObjectName(abstractElement)));
            }
        } else {
            if (precision != null) {
                throw validator.newPositionalError(
                    abstractElement,
                    validator.res.newValidatorPrecUnexpected(
                        repos.getLocalizedObjectName(type),
                        repos.getLocalizedObjectName(abstractElement)));
            }
        }
        if ((typeName != null) && typeName.allowsScale()) {
            // assume scale is always optional
        } else {
            if (element.getScale() != null) {
                throw validator.newPositionalError(
                    abstractElement,
                    validator.res.newValidatorScaleUnexpected(
                        repos.getLocalizedObjectName(type),
                        repos.getLocalizedObjectName(abstractElement)));
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
                        abstractElement,
                        validator.res.newValidatorCharsetUnsupported(
                            element.getCharacterSetName(),
                            repos.getLocalizedObjectName(abstractElement)));
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
                    abstractElement,
                    validator.res.newValidatorCharsetUnexpected(
                        repos.getLocalizedObjectName(type),
                        repos.getLocalizedObjectName(abstractElement)));
            }
        }

        // now, enforce type-defined limits
        if (type instanceof CwmSqlsimpleType) {
            CwmSqlsimpleType simpleType = (CwmSqlsimpleType) type;

            if (element.getLength() != null) {
                Integer maximum = simpleType.getCharacterMaximumLength();
                assert (maximum != null);
                if (element.getLength().intValue() > maximum.intValue()) {
                    throw validator.newPositionalError(
                        abstractElement,
                        validator.res.newValidatorLengthExceeded(
                            element.getLength(),
                            maximum,
                            repos.getLocalizedObjectName(abstractElement)));
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
                        abstractElement,
                        validator.res.newValidatorPrecisionExceeded(
                            element.getPrecision(),
                            maximum,
                            repos.getLocalizedObjectName(abstractElement)));
                }
            }
            if (element.getScale() != null) {
                Integer maximum = simpleType.getNumericScale();
                assert (maximum != null);
                if (element.getScale().intValue() > maximum.intValue()) {
                    throw validator.newPositionalError(
                        abstractElement,
                        validator.res.newValidatorScaleExceeded(
                            element.getScale(),
                            maximum,
                            repos.getLocalizedObjectName(abstractElement)));
                }
            }
        } else if (type instanceof FemSqlcollectionType) {
            FemSqlcollectionType collectionType =
                (FemSqlcollectionType) type;
            FemSqltypeAttribute componentType = (FemSqltypeAttribute)
                collectionType.getFeature().get(0);
            validateAttribute(componentType);
        } else if (type instanceof FemUserDefinedType) {
            // nothing special to do for UDT's, which were
            // already validated on creation
        } else {
            throw Util.needToImplement(type);
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

        validator.getStmtValidator().setParserPosition(null);
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
        CwmSqldataType cwmType = 
            validator.getStmtValidator().findSqldataType(
                type.getSqlIdentifier());
        column.setType(cwmType);
        SqlTypeName typeName = type.getSqlTypeName();
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
    
    public Throwable adjustExceptionParserPosition(
        CwmModelElement modelElement,
        Throwable ex)
    {
        if (ex instanceof FarragoException) {
            FarragoException contextExcn = (FarragoException) ex;
            if (contextExcn.getPosLine() != 0) {
                // We have context information for the query, and
                // need to adjust the position to match the original
                // DDL statement.
                SqlParserPos offsetPos = validator.getParserOffset(
                    modelElement);
                int line = contextExcn.getPosLine();
                int col = contextExcn.getPosColumn();
                if (line == 1) {
                    col += (offsetPos.getColumnNum() - 1);
                }
                line += (offsetPos.getLineNum() - 1);
                ex = EigenbaseResource.instance().newValidatorContext(
                    new Integer(line),
                    new Integer(col),
                    ex.getCause());
            }
        }
        return ex;
    }
    
    private void validateDefaultClause(
        FemAbstractAttribute attribute,
        FarragoSession session,
        String defaultExpression)
    {
        String sql = "VALUES(" + defaultExpression + ")";
        FarragoSessionStmtContext stmtContext = session.newStmtContext();
        stmtContext.prepare(sql, false);
        RelDataType rowType = stmtContext.getPreparedRowType();
        assert (rowType.getFieldList().size() == 1);

        if (stmtContext.getPreparedParamType().getFieldList().size() > 0) {
            throw validator.newPositionalError(
                attribute,
                validator.res.newValidatorBadDefaultParam(
                    repos.getLocalizedObjectName(attribute)));
        }

        // SQL standard is very picky about what can go in a DEFAULT clause
        RelDataType sourceType = rowType.getFields()[0].getType();
        RelDataTypeFamily sourceTypeFamily = sourceType.getFamily();

        RelDataType targetType =
            validator.getTypeFactory().createCwmElementType(attribute);
        RelDataTypeFamily targetTypeFamily = targetType.getFamily();

        if (sourceTypeFamily != targetTypeFamily) {
            throw validator.newPositionalError(
                attribute,
                validator.res.newValidatorBadDefaultType(
                    repos.getLocalizedObjectName(attribute),
                    targetTypeFamily.toString(),
                    sourceTypeFamily.toString()));
        }

        // TODO:  additional rules from standard, like no truncation allowed.
        // Maybe just execute SELECT with and without cast to target type and
        // make sure the same value comes back.
    }
}

// End DdlHandler.java
