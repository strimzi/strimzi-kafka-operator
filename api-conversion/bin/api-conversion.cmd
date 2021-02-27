@echo off

setlocal

REM make it work in the release
SET CLASSPATH=%~dp0..\libs\*;%CLASSPATH%

@REM Check if JAVA_HOME is set
if not defined JAVA_HOME (
  echo Error: JAVA_HOME is not set.
  goto :eof
)

set JAVA_HOME=%JAVA_HOME:"=%

if not exist "%JAVA_HOME%"\bin\java.exe (
  echo Error: JAVA_HOME is incorrectly set.
  goto :eof
)

REM strip off trailing \ from JAVA_HOME or java does not start
if "%JAVA_HOME:~-1%" EQU "\" set "JAVA_HOME=%JAVA_HOME:~0,-1%"

set JAVA="%JAVA_HOME%"\bin\java

call %JAVA% -cp "%CLASSPATH%" io.strimzi.kafka.api.conversion.cli.EntryCommand %*

endlocal