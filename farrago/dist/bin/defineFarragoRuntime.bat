rem $Id$
rem Define variables needed by runtime scripts such as farragoServer.bat.
rem This script is meant to be sourced from other scripts, not
rem executed directly.

set MAIN_DIR=%~dp0..

if not exist "%MAIN_DIR%\bin\classpath.bat" goto need_install

call "%MAIN_DIR%\bin\classpath.bat"

rem If you are trying to give additional memory usable by queries
rem see this doc: http://pub.eigenbase.org/wiki/LucidDbBufferPoolSizing
rem Upping Java Heap will unlikely help queries on "large" datasets
set JAVA_ARGS=-Xms256m -Xmx256m -XX:MaxPermSize=128m -cp %LCP% -Dnet.sf.farrago.home="%MAIN_DIR%" -Djava.util.logging.config.file="%MAIN_DIR%\trace\Trace.properties"

set SQLLINE_JAVA_ARGS=sqlline.SqlLine

set JAVA_EXEC=%JAVA_HOME%\bin\java

set PATH=%PATH%;%MAIN_DIR%\plugin;%MAIN_DIR%\lib\fennel

goto done

:need_install
    echo Error:  "%MAIN_DIR%\install\install.bat" has not been run yet.
    exit /b 1

:done

