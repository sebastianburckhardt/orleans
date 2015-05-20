@Echo OFF
@setlocal

IF %1.==. GOTO Usage
IF %2.==. GOTO Usage

@REM If specified, valid values for RELEASE_TYPE are: alpha|beta|rc|rtm - See https://ms-nuget.cloudapp.net/policies/Faq
@REM set RELEASE_TYPE=alpha

set NUGET_EXE=%~dp0..\..\dependencies\NuGet\nuget.exe
set BASE_PATH=%1
set VERSION=%2
if EXIST %VERSION% (
@Echo Reading version number from file %VERSION%
@REM FOR /F "tokens=1,2,3,4 delims=." %%i in (%VERSION%) do @set VERSION=%%i.%%j.%%k-%RELEASE_TYPE%-%%l
FOR /F "tokens=1,2,3,4 delims=." %%i in (%VERSION%) do @set VERSION=%%i.%%j.%%k.%%l
)
@echo CreateOrleansNugetPackages %VERSION%
@echo SDK drop location: "%BASE_PATH%"

"%NUGET_EXE%" SetApiKey 4a4a0b74-e179-4f06-b92b-c5dcdefb7321
"%NUGET_EXE%" pack "%~dp0\Microsoft.XCG.Orleans.nuspec" -Version "%VERSION%" -BasePath "%BASE_PATH%" -Verbose
"%NUGET_EXE%" pack "%~dp0\Microsoft.XCG.Orleans.Client.nuspec" -Version "%VERSION%" -BasePath "%BASE_PATH%" -Verbose
"%NUGET_EXE%" pack "%~dp0\Microsoft.XCG.Orleans.Host.nuspec" -Version "%VERSION%" -BasePath "%BASE_PATH%" -Verbose

GOTO EOF

:Usage
ECHO Usage:
ECHO    CreateOrleansPackages ^<Path to Orleans SDK folder^> ^<Version^>

:EOF