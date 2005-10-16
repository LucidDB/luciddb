/*
// $Id$
// Package org.eigenbase is a class library of data management components.
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
package org.eigenbase.test;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.resource.*;
import org.eigenbase.resgen.*;
import org.eigenbase.util.*;

/**
 * SqlValidatorFeatureTest verifies that features can be
 * independently enabled or disabled.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlValidatorFeatureTest extends SqlValidatorTestCase
{
    private static final String FEATURE_DISABLED = "feature_disabled";
    
    private ResourceDefinition disabledFeature;
        
    /**
     * Returns a tester. Derived classes should override this method to run
     * the same set of tests in a different testing environment.
     */
    public Tester getTester()
    {
        return new FeatureTesterImpl();
    }

    private class FeatureTesterImpl extends TesterImpl
    {
        public SqlValidator getValidator()
        {
            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl();
            return new FeatureValidator(
                SqlStdOperatorTable.instance(),
                new MockCatalogReader(typeFactory),
                typeFactory);
        }
    }

    private class FeatureValidator extends SqlValidatorImpl
    {
        protected FeatureValidator(
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory)
        {
            super(opTab, catalogReader, typeFactory);
        }

        protected void validateFeature(
            ResourceDefinition feature,
            SqlParserPos context)
        {
            if (feature == disabledFeature) {
                EigenbaseException ex = new EigenbaseException(
                    FEATURE_DISABLED, null);
                SqlParserPos pos = null;
                if (context == null) {
                    throw ex;
                }
                throw new EigenbaseContextException(
                    "location",
                    ex,
                    context.getLineNum(),
                    context.getColumnNum(),
                    context.getEndLineNum(),
                    context.getEndColumnNum());
            }
        }
    }

    public void testDistinct()
    {
        checkFeature(
            "select ^distinct^ name from dept",
            EigenbaseResource.instance().SQLFeature_E051_01);
    }

    private void checkFeature(String sql, ResourceDefinition feature)
    {
        // Test once with feature enabled:  should pass
        check(sql);

        // Test once with feature disabled:  should fail
        try {
            disabledFeature = feature;
            checkFails(sql, FEATURE_DISABLED);
        } finally {
            disabledFeature = null;
        }
    }
}

// End SqlValidatorFeatureTest.java
