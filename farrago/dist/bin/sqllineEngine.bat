@echo off
rem $Id$
rem Run the sqlline command-line SQL interpreter 
rem with an embedded Farrago engine

setlocal
set MAIN_DIR=%~dp0..

call "%MAIN_DIR%\bin\defineFarragoRuntime.bat"
if errorlevel 1 goto done

%JAVA_EXEC% %JAVA_ARGS% %SQLLINE_JAVA_ARGS% -u jdbc:farrago: -d net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver -n guest %*

:done
