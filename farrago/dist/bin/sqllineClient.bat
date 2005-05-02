@echo off
rem $Id$
rem Run the sqlline command-line SQL interpreter as a client
rem to a Farrago server

setlocal
set MAIN_DIR=%~dp0..

call %MAIN_DIR%/bin/defineFarragoRuntime.bat
if errorlevel 1 goto done

%JAVA_EXEC% %JAVA_ARGS% %SQLLINE_JAVA_ARGS% -u jdbc:farrago:rmi://localhost -d net.sf.farrago.jdbc.client.FarragoJdbcClientDriver -n guest %*

:done
