@REM
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.  See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership.  The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License.  You may obtain a copy of the License at
@REM
@REM     http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.
@REM

@echo off
echo ````````````````````````
echo Starting iot-benchmark
echo ````````````````````````


set PATH="%JAVA_HOME%\bin\";%PATH%
set "FULL_VERSION="
set "MAJOR_VERSION="
set "MINOR_VERSION="


for /f tokens^=2-5^ delims^=.-_+^" %%j in ('java -fullversion 2^>^&1') do (
	set "FULL_VERSION=%%j-%%k-%%l-%%m"
	IF "%%j" == "1" (
	    set "MAJOR_VERSION=%%k"
	    set "MINOR_VERSION=%%l"
	) else (
	    set "MAJOR_VERSION=%%j"
	    set "MINOR_VERSION=%%k"
	)
)

set JAVA_VERSION=%MAJOR_VERSION%

@REM we do not check jdk that version less than 1.6 because they are too stale...
IF "%JAVA_VERSION%" == "6" (
		echo IoTDB only supports jdk >= 8, please check your java version.
		goto finally
)
IF "%JAVA_VERSION%" == "7" (
		echo IoTDB only supports jdk >= 8, please check your java version.
		goto finally
)

if "%OS%" == "Windows_NT" setlocal

if NOT DEFINED BENCHMARK_HOME set BENCHMARK_HOME=%~dp0
set BENCHMARK_CONF=%BENCHMARK_HOME%\conf
set BENCHMARK_LOGS=%BENCHMARK_HOME%\logs

if NOT DEFINED MAIN_CLASS set MAIN_CLASS=cn.edu.tsinghua.iot.benchmark.App
if NOT DEFINED JAVA_HOME goto :err

@REM -----------------------------------------------------------------------------
@REM JVM Opts we'll use in legacy run or installation
set JAVA_OPTS=-ea^
 -Dlogback.configurationFile="%BENCHMARK_CONF%\logback.xml"^
 -Dsun.jnu.encoding=UTF-8^
 -Dfile.encoding=UTF-8

@REM ***** CLASSPATH library setting *****
@REM Ensure that any user defined CLASSPATH variables are not used on startup
@REM all-in-one 布局（存在 lib\core 目录）时：先加载 DB_SWITCH 对应的专属目录
@REM lib\<db>，再加载 lib\core；旧布局（lib 直接放 jar）保持原行为。
set CLASSPATH=
set DB_LIB_DIR=
if exist "%BENCHMARK_HOME%\lib\core\" (
  for /f "tokens=1,* delims==" %%a in ('findstr /b /c:"DB_SWITCH=" "%BENCHMARK_CONF%\config.properties" 2^>nul') do set "DB_SWITCH=%%b"
  if not defined DB_SWITCH for /f "tokens=1,* delims==" %%a in ('findstr /b /c:"# DB_SWITCH=" "%BENCHMARK_CONF%\config.properties" 2^>nul') do set "DB_SWITCH=%%b"
  if not defined DB_SWITCH for /f "tokens=1,* delims==" %%a in ('findstr /b /c:"#DB_SWITCH=" "%BENCHMARK_CONF%\config.properties" 2^>nul') do set "DB_SWITCH=%%b"
  if defined DB_SWITCH set "DB_SWITCH=%DB_SWITCH: =%"
  if defined DB_SWITCH call :select_db_lib
  if defined DB_LIB_DIR if exist "%BENCHMARK_HOME%\lib\%DB_LIB_DIR%\" (
    for %%f in ("%BENCHMARK_HOME%\lib\%DB_LIB_DIR%\*.jar") do call :append "%%~f"
  )
  for %%f in ("%BENCHMARK_HOME%\lib\core\*.jar") do call :append "%%~f"
) else (
  set CLASSPATH="%BENCHMARK_HOME%\lib\*"
)
goto okClasspath

:select_db_lib
echo.%DB_SWITCH%|findstr /b /c:"IoTDB-200-" >nul && set "DB_LIB_DIR=iotdb-2.0" && goto :eof
echo.%DB_SWITCH%|findstr /b /c:"IoTDB-130-" >nul && set "DB_LIB_DIR=iotdb-1.3" && goto :eof
echo.%DB_SWITCH%|findstr /b /c:"InfluxDB-2" >nul && set "DB_LIB_DIR=influxdb-2.0" && goto :eof
echo.%DB_SWITCH%|findstr /b /c:"InfluxDB" >nul && set "DB_LIB_DIR=influxdb" && goto :eof
echo.%DB_SWITCH%|findstr /b /c:"OpenTSDB" >nul && set "DB_LIB_DIR=opentsdb" && goto :eof
echo.%DB_SWITCH%|findstr /b /c:"CnosDB" >nul && set "DB_LIB_DIR=cnosdb" && goto :eof
echo.%DB_SWITCH%|findstr /b /c:"KairosDB" >nul && set "DB_LIB_DIR=kairosdb" && goto :eof
echo.%DB_SWITCH%|findstr /b /c:"TimescaleDB-cluster" >nul && set "DB_LIB_DIR=timescaledb-cluster" && goto :eof
echo.%DB_SWITCH%|findstr /b /c:"TimescaleDB" >nul && set "DB_LIB_DIR=timescaledb" && goto :eof
echo.%DB_SWITCH%|findstr /b /c:"TDengine-3" >nul && set "DB_LIB_DIR=tdengine-3.0" && goto :eof
echo.%DB_SWITCH%|findstr /b /c:"TDengine" >nul && set "DB_LIB_DIR=tdengine" && goto :eof
echo.%DB_SWITCH%|findstr /b /c:"QuestDB" >nul && set "DB_LIB_DIR=questdb" && goto :eof
echo.%DB_SWITCH%|findstr /b /c:"MsSqlServer" >nul && set "DB_LIB_DIR=mssqlserver" && goto :eof
echo.%DB_SWITCH%|findstr /b /c:"VictoriaMetrics" >nul && set "DB_LIB_DIR=victoriametrics" && goto :eof
echo.%DB_SWITCH%|findstr /b /c:"DolphinDB-3" >nul && set "DB_LIB_DIR=dolphindb-3.0" && goto :eof
echo.%DB_SWITCH%|findstr /b /c:"DolphinDB-2" >nul && set "DB_LIB_DIR=dolphindb-2.0" && goto :eof
echo.%DB_SWITCH%|findstr /b /c:"SQLite" >nul && set "DB_LIB_DIR=sqlite" && goto :eof
goto :eof

:append
set CLASSPATH=%CLASSPATH%;%~1

goto :eof

@REM -----------------------------------------------------------------------------
:okClasspath

rem echo CLASSPATH: %CLASSPATH%

"%JAVA_HOME%\bin\java" %JAVA_OPTS% -cp "%CLASSPATH%" %MAIN_CLASS% -cf %BENCHMARK_HOME%/conf
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause


@REM -----------------------------------------------------------------------------
:finally

pause

ENDLOCAL
