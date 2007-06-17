/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import java.io.*;

import java.lang.reflect.*;

import java.nio.charset.*;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * Utility to generate a SQL script from validator test.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 10, 2004
 */
public class SqlTestGen
{
    //~ Methods ----------------------------------------------------------------

    public static void main(String [] args)
    {
        new SqlTestGen().genValidatorTest();
    }

    private void genValidatorTest()
    {
        FileOutputStream fos = null;
        PrintWriter pw = null;
        try {
            File file = new File("validatorTest.sql");
            fos = new FileOutputStream(file);
            pw = new PrintWriter(fos);
            Method [] methods = getJunitMethods(SqlValidatorSpooler.class);
            for (int i = 0; i < methods.length; i++) {
                Method method = methods[i];
                final SqlValidatorSpooler test =
                    new SqlValidatorSpooler(
                        method.getName(),
                        pw);
                final Object result =
                    method.invoke(
                        test,
                        new Object[0]);
                assert result == null;
            }
        } catch (IOException e) {
            throw Util.newInternal(e);
        } catch (IllegalAccessException e) {
            throw Util.newInternal(e);
        } catch (IllegalArgumentException e) {
            throw Util.newInternal(e);
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            throw Util.newInternal(e);
        } finally {
            if (pw != null) {
                pw.flush();
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    throw Util.newInternal(e);
                }
            }
        }
    }

    /**
     * Returns a list of all of the Junit methods in a given class.
     */
    private static Method [] getJunitMethods(Class<SqlValidatorSpooler> clazz)
    {
        List<Method> list = new ArrayList<Method>();
        for (Method method : clazz.getMethods()) {
            if (method.getName().startsWith("test")
                && Modifier.isPublic(method.getModifiers())
                && !Modifier.isStatic(method.getModifiers())
                && (method.getParameterTypes().length == 0)
                && (method.getReturnType() == Void.TYPE))
            {
                list.add(method);
            }
        }
        return list.toArray(new Method[list.size()]);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Subversive subclass, which spools restuls to a writer rather than running
     * tests. It is not a valid JUnit test because it does not have a public
     * constructor.
     */
    private static class SqlValidatorSpooler
        extends SqlValidatorTest
    {
        private final PrintWriter pw;

        private SqlValidatorSpooler(String testName, PrintWriter pw)
        {
            super(testName);
            this.pw = pw;
        }

        public SqlValidatorTestCase.Tester getTester()
        {
            return new TesterImpl() {
                public SqlValidator getValidator()
                {
                    throw new UnsupportedOperationException();
                }

                public void assertExceptionIsThrown(
                    String sql,
                    String expectedMsgPattern)
                {
                    if (expectedMsgPattern == null) {
                        // This SQL statement is supposed to succeed.
                        // Generate it to the file, so we can see what
                        // output it produces.
                        pw.println("-- " + getName());
                        pw.println(sql);
                        pw.println(";");
                    } else {
                        // Do nothing. We know that this fails the validator
                        // test, so we don't learn anything by having it fail
                        // from SQL.
                    }
                }

                public RelDataType getColumnType(String sql)
                {
                    return null;
                }

                public void checkType(
                    String sql,
                    String expected)
                {
                    // We could generate the SQL -- or maybe describe -- but
                    // ignore it for now.
                }

                public void checkCollation(
                    String sql,
                    String expectedCollationName,
                    SqlCollation.Coercibility expectedCoercibility)
                {
                    // We could generate the SQL -- or maybe describe -- but
                    // ignore it for now.
                }

                public void checkCharset(
                    String sql,
                    Charset expectedCharset)
                {
                    // We could generate the SQL -- or maybe describe -- but
                    // ignore it for now.
                }
            };
        }
    }
}

// End SqlTestGen.java
