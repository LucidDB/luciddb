/*
// Farrago is a relational database management system.
// (C) Copyright 2004-2004, Disruptive Tech
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
package net.sf.farrago.test.regression;

/**
 * FarragoTestCommand represents a command, sequentially executed by
 * {@link FarragoTestCommandExecutor}, during a concurrency test
 * ({@link FarragoConcurrencyTestCase}.
 *
 * <p>FarragoTestCommand instances are normally instantiated by the
 * {@link FarragoTestCommandGenerator} class.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public interface FarragoTestCommand
{
    /**
     * Executes this command.  The FarragoTestCommandExecutor provides
     * access to a JDBC connection and previously prepared statements.
     *
     * @param exec the FarragoTestCommandExecutor firing this command.
     * @see FarragoTestCommandExecutor#getStatement()
     * @see FarragoTestCommandExecutor#setStatement(java.sql.Statement)
     * @throws Exception to indicate a test failure
     */
    void execute(FarragoTestCommandExecutor exec)
        throws Exception;
}
