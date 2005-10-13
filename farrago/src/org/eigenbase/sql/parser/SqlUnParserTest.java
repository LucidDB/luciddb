/*
// $Id$
// Aspen dataflow server
// (C) Copyright 2004-2004 Disruptive Technologies, Inc.
*/
package org.eigenbase.sql.parser;

import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlUtil;

/**
 * Extension to {@link SqlParserTest} which ensures that every expression
 * can un-parse successfully.
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 12, 2005
 */
public class SqlUnParserTest extends SqlParserTest
{
    public SqlUnParserTest(String name)
    {
        super(name);
    }

    protected Tester getTester()
    {
        return new UnparsingTesterImpl();
    }

}

// End SqlUnParserTest.java