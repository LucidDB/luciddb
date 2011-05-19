/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2011 Dynamo Business Intelligence Corporation

// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by Dynamo Business Intelligence Corporation.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
*/

package org.eigenbase.applib.script;

import net.sf.farrago.util.*;
import net.sf.farrago.resource.*;

import javax.script.*;
import java.io.*;
import java.sql.*;

/**
 * Provides a basic class to run scripts from LucidDB routines.
 * in languages such as Python, JavaScript, or Clojure.
 * All functions support ${FARRAGO_HOME} in the script-as-file parameter,
 * and the path variable APPLIB_JAR is available for all scripts.
 *
 * @author Kevin Secretan
 * @version $Id$
 */
public abstract class ExecuteScriptUdr
{

  /**
   * Runs script code as a CALL procedure.
   *
   * @param engineName - script engine to use, e.g. "python" for Jython.
   * @param script - either a string to evaluate or a filename to load and eval.
   */
  public static void executeUdp(String engineName, String script)
    throws SQLException, FileNotFoundException, ScriptException
  {
    getEngineAndEval(engineName, script);
  }


  /**
   * Executes a parameter-less function defined in a given script. The
   * default function name is "execute".
   *
   * @param engineName - script engine to use, e.g. "python" for Jython.
   * @param script - either a string to evaluate or a filename to load and eval.
   * @param funcName - function to call.
   * @return Returns the result of the function call as a String.
   */
  public static String executeUdf(
      String engineName,
      String script,
      String funcName)
    throws SQLException, FileNotFoundException, ScriptException,
                    NoSuchMethodException

  {
    ScriptEngine engine = getEngineAndEval(engineName, script);
    Invocable inv = (Invocable) engine;
    Object result = inv.invokeFunction(funcName);
    return (result == null) ? "" : result.toString();
  }

  public static String executeUdf(String engineName, String script)
    throws SQLException, FileNotFoundException, ScriptException,
                    NoSuchMethodException

  {
    return executeUdf(engineName, script, "execute");
  }

  /**
   * Executes a parameter-less object method defined in a given script.
   *
   * @param engineName - script engine to use, e.g. "python" for Jython.
   * @param script - either a string to evaluate or a filename to load and eval.
   * @param objName - object to call method on. Note this could be a class.
   * @param methodName - method to call.
   * @return Returns the result of the method call as a String.
   */
  public static String executeUdf(
      String engineName,
      String script,
      String objName,
      String methodName)
    throws SQLException, FileNotFoundException, ScriptException,
                    NoSuchMethodException
  {
    ScriptEngine engine = getEngineAndEval(engineName, script);
    Invocable inv = (Invocable) engine;
    Object obj = engine.get(objName);
    Object result = inv.invokeMethod(obj, methodName);
    return (result == null) ? "" : result.toString();
  }

  // Helper functions

  /**
   * Evaluates a given script and, if the caller desires, returns
   * the ScriptEngine object for further use.
   */
  public static ScriptEngine getEngineAndEval(String engineName, String script)
    throws SQLException, FileNotFoundException, ScriptException
  {
    ScriptEngine engine = getScriptEngine(engineName);
    evalScript(engine, script);
    return engine;
  }

  /**
   * Gets the script evaluation engine defined by JSR223.
   * @param engineName - script engine to use
   * @return Returns a ScriptEngine instance if engine is found.
   * @throws Throws an SQLException if engine is not found.
   */
  public static ScriptEngine getScriptEngine(String engineName)
    throws SQLException
  {
    ScriptEngine engine = new ScriptEngineManager().getEngineByName(engineName);
    if (engine == null) {
      throw new SQLException(
          FarragoResource.instance().ValidatorUnknownObject.ex(
            "script engine " + engineName + ". Is it on Farrago's CLASSPATH?"
            ).getMessage());
    }
    return engine;
  }

  /**
   * Evaluates a script given an engine, with the script being a raw string
   * or a file location.
   */
  public static void evalScript(ScriptEngine engine, String script)
    throws FileNotFoundException, ScriptException
  {
    // Make this variable available so scripts can import things from
    // this jar.
    String APPLIB_JAR = FarragoProperties.instance().expandProperties(
        "${FARRAGO_HOME}/plugin/eigenbase-applib.jar");
    engine.put("APPLIB_JAR", APPLIB_JAR);

    String parsed = FarragoProperties.instance().expandProperties(script);
    File file = new File(parsed);
    if (file.exists()) {
      engine.eval(new BufferedReader(new FileReader(file)));
    } else {
      engine.eval(parsed);
    }
  }

  public static void getScriptingEngines(PreparedStatement resultInserter)
    throws SQLException
  {
    ScriptEngineManager manager = new ScriptEngineManager();
    for (ScriptEngineFactory factory : manager.getEngineFactories()) {
      for (String name : factory.getNames()) {
        int c = 0;
        resultInserter.setString(++c, factory.getEngineName());
        resultInserter.setString(++c, factory.getEngineVersion());
        resultInserter.setString(++c, factory.getLanguageName());
        resultInserter.setString(++c, factory.getLanguageVersion());
        resultInserter.setString(++c, name);
        resultInserter.executeUpdate();
      }
      resultInserter.executeUpdate();
    }
  }

}
