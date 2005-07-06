@echo off
rem $Id$
rem Run the iSQLViewer GUI client to a Farrago server

setlocal
set MAIN_DIR=%~dp0..

call "%MAIN_DIR%\bin\defineFarragoRuntime.bat"
if errorlevel 1 goto done

%JAVA_EXEC% -Disql.home=%MAIN_DIR%\isql -cp %LCP%;%MAIN_DIR%\isql\isql-core.jar org.isqlviewer.core.Launcher %*

:done
