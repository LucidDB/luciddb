/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
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
package net.sf.saffron.test;

import net.sf.saffron.core.SaffronTypeFactoryImpl;
import net.sf.saffron.sql.SqlOperatorTable;
import net.sf.saffron.sql.SqlValidator;
import net.sf.saffron.sql.parser.ParseException;
import net.sf.saffron.sql.parser.SqlParser;

/**
 * ...
 *
 * @author wael
 * @since Jan 14, 2004
 * @version $Id$
 **/
public class SaffronSqlValidationTest extends SqlValidatorTestCase{
	//inner classes
	public class SaffronTestCatalogReader implements SqlValidator.CatalogReader {
		public SqlValidator.Table getTable(String[] names) {
			return null;
		}
	}

	public SqlParser getParser(String sql) throws ParseException {
		return new SqlParser(sql);
	}
	public SqlValidator getValidator() {
        return new SqlValidator(SqlOperatorTable.instance(),
                new SaffronTestCatalogReader(),
                new SaffronTypeFactoryImpl());
	}
}

// End SaffronSqlValidationTest.java