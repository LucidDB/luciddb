@rem $Id$
@rem Saffron preprocessor and data engine
@rem (C) Copyright 2002-2004 Disruptive Technologies, Inc.
@rem You must accept the terms in LICENSE.html to use this software.

@set SRCROOT=%~dp0
@set HOME=E:\
@set ANT_HOME=%HOME%apache-ant-1.6.0
@set MACKER_HOME=%HOME%macker-0.4.1
@set JALOPY_HOME=%HOME%jalopy-0.6.1
@set JAVACC_HOME=%HOME%javacc-3.2
@set JUNIT_HOME=%HOME%junit3.8.1

@rem The following 3 components are optional; uncomment them if you have them.
@rem set TOMCAT_HOME=%HOME%jakarta-tomcat-4.1.24
@rem set DYNAMICJAVA_HOME=%HOME%DynamicJava-1.1.4
@rem set WEBLOGIC_HOME=%HOME%bea/wlserver6.1

@set CLASSPATH=%JAVACC_HOME%\bin\lib\javacc.jar;%SRCROOT%\classes;%SRCROOT%\lib\boot.jar
@%ANT_HOME%\bin\ant %1 %2 %3 %4 %5 %6 %7 %8 %9

@rem End build.bat
