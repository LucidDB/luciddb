@echo off
rem setlocal

if "%JAVA_HOME%" == "" goto need_java

@echo on

"%JAVA_HOME%\bin\java.exe" -classpath ".\lib\LucidDBClient.jar;.\build\PG2LucidDB.jar;.\lib\hsqldb.jar;.\lib\log4j.jar;" -DPG2LucidDB.logger=conf/log4j.properties org.luciddb.pg2luciddb.Server conf/PG2LucidDB.properties 

goto end

:need_java
    echo "The JAVA_HOME environment variable must be set to the location"
    echo "of a version 1.6 JVM."
:end
