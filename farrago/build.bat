@rem Farrago transactional SQL DBMS
@rem $Id$
@rem Copyright (C) 2002-2009 Julian Hyde

@set SRCROOT=%~dp0
@set HOME_DRIVE=E
@set JAVACC_HOME=%HOME_DRIVE%:\javacc-3.2
@set ANT_HOME=%HOME_DRIVE%:\apache-ant-1.6.0
@set JUNIT_HOME=%HOME_DRIVE%:/junit3.8.1

@set CLASSPATH=%JAVACC_HOME%\bin\lib\JavaCC.zip;%SRCROOT%\classes
@%ANT_HOME%\bin\ant %1 %2 %3 %4 %5 %6 %7 %8 %9

@rem End build.bat
