@echo off
rem $Id$
rem Run Farrago as a standalone RMI server

setlocal
set MAIN_DIR=%~dp0..

call "%MAIN_DIR%\bin\defineFarragoRuntime.bat"
if errorlevel 1 goto done

%JAVA_EXEC% %JAVA_ARGS% net.sf.farrago.server.FarragoVjdbcServer

:done
