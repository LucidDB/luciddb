@echo off
rem $Id$
rem Farrago installation script

setlocal
set BASE_DIR=%~dp0..
set LIB_DIR=%BASE_DIR%\lib
set BIN_DIR=%BASE_DIR%\bin
set TRACE_DIR=%BASE_DIR%\trace
set TRACE_CONFIG=%TRACE_DIR%\Trace.properties

if "%JAVA_HOME%" == "" goto need_java

if not exist "%TRACE_DIR%" mkdir "%TRACE_DIR%"
echo # Tracing configuration> "%TRACE_CONFIG%"
echo handlers=java.util.logging.FileHandler>> "%TRACE_CONFIG%"
echo java.util.logging.FileHandler.append=true>> "%TRACE_CONFIG%"
echo java.util.logging.FileHandler.formatter=java.util.logging.SimpleFormatter>> "%TRACE_CONFIG%"
echo java.util.logging.FileHandler.pattern=%TRACE_DIR:\=\\%\\Trace.log>> "%TRACE_CONFIG%"
echo .level=CONFIG>> "%TRACE_CONFIG%"

echo set JAVA_HOME="%JAVA_HOME%"> "%BIN_DIR%\classpath.bat"
echo set LCP="%JAVA_HOME%\lib\tools.jar">> "%BIN_DIR%\classpath.bat"
for %%j in ("%LIB_DIR%\*.jar") do echo set LCP=%%LCP%%;"%%j">> "%BIN_DIR%\classpath.bat"
for %%j in ("%LIB_DIR%\mdrlibs\*.jar") do echo set LCP=%%LCP%%;"%%j">> "%BIN_DIR%\classpath.bat"

echo Installation successful

goto end

:need_java
    echo "The JAVA_HOME environment variable must be set to the location"
    echo "of your desired JVM, JDK Version 1.4 or newer."

:end
