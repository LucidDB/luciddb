/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
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

  /**
   * Executes a function defined in a given script, passing it an argument
   * array and returning the result as a string.
   *
   * @param engineName - script engine to use
   * @param script - either a string to evaluate or a filename to load and eval.
   * @param funcName - function to call.
   * @param args - arguments passed to function.
   * @return Returns the result of the function call as a String.
   */
  public static String executeGeneralUdf(
      String engineName,
      String script,
      String funcName,
      String ...args)
    throws SQLException, FileNotFoundException, ScriptException,
                    NoSuchMethodException
  {
    ScriptEngine engine = getEngineAndEval(engineName, script);
    Invocable inv = (Invocable) engine;
    Object result = inv.invokeFunction(funcName, (Object)args);
    return (result == null) ? "" : result.toString();
  }

  /**
   * Possible helper function for the above, but neither are in use right now
   * since LucidDB cannot pass arrays unless they're from a cursor.
   */
  public static String executeGeneralUdf(
      String engineName,
      String script,
      String funcName,
      java.util.List<String> args)
    throws SQLException, FileNotFoundException, ScriptException,
                    NoSuchMethodException
  {
    return executeGeneralUdf(
        engineName, script, funcName, args.toArray(new String[args.size()]));
  }

  /**
   * Executes a script as a UDX, making the two variables inputSet and
   * resultInserter available globally for the script.
   *
   * @param engineName - script engine to use
   * @param script - either a string to evaluate or a filename to load and eval.
   * @param inputSet - made available to the script.
   * @param resultInserter - made available to the script.
   */
  public static void executeUdx(
      String engineName,
      String script,
      ResultSet inputSet,
      PreparedStatement resultInserter)
    throws SQLException, FileNotFoundException, ScriptException,
                    NoSuchMethodException
  {
    ScriptEngine engine = getScriptEngine(engineName);
    engine.put("inputSet", inputSet);
    engine.put("resultInserter", resultInserter);
    evalScript(engine, script);
  }

  /**
   * Gets a list of available scripting engines for LucidDB to use.
   */
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

}
