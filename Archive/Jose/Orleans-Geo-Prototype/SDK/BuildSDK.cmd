@setlocal
@echo off
@if NOT "%ECHO%"=="" @echo %ECHO%

if "%1"=="" (
  set OUTDIR=C:\CCF\Orleans\Code\Binaries\SDK-DROP
) else (
  set OUTDIR=%1
)
if "%2"=="" (
  set INDIR=.
) else (
  set INDIR=%2
)

set CMDHOME=%~dp0.

set LOCAL_RUNTIME=%OUTDIR%\LocalSilo
set APPDIR=%LOCAL_RUNTIME%\Applications
set APPBINDIR=%OUTDIR%\Binaries\Applications
set DEPLOYCFGDIR=%OUTDIR%\RemoteDeployment
set UTILSDIR=%OUTDIR%\Utils

set ORL_SERVER_BINARIES=%OUTDIR%\Binaries\OrleansServer
set ORL_CLIENT_BINARIES=%OUTDIR%\Binaries\OrleansClient
@ECHO set WSP_ROUTER_BINARIES=%OUTDIR%\Binaries\WspEventRouterSetup

set SAMPLESOUT=%OUTDIR%\Samples
set SAMPLESRC=%INDIR%\Samples

@REM set TOOLSDIR=%OUTDIR%\Tools
@REM set MANAGERDIR=%TOOLSDIR%\Manager
@REM set VS_TEMPLATES=%TOOLSDIR%\VisualStudioTemplates
set VS_TEMPLATES=%OUTDIR%\VisualStudioTemplates
set DOCSDIR=%OUTDIR%\Docs

@echo == Building Orleans SDK drop to %OUTDIR%

@echo InDir=%INDIR%
@echo OutDir=%OUTDIR%
@echo AppDir=%APPDIR%
@echo LocalSilo=%LOCAL_RUNTIME%

set D="%OUTDIR%"
if not exist "%D%" (md "%D%")

@ECHO == Copy Orleans Server Binaries
set D="%ORL_SERVER_BINARIES%"
if not exist "%D%" (md "%D%")
set D="%LOCAL_RUNTIME%"
if not exist "%D%" (md "%D%")
 if not exist "%D%\Applications" (md "%D%\Applications")
xcopy /y /s "%INDIR%\Orleans\*" "%ORL_SERVER_BINARIES%\"
xcopy /y /s "%INDIR%\Orleans\*" "%LOCAL_RUNTIME%\"
copy /y "%INDIR%\Orleans-SDK\Build-Number.txt" "%LOCAL_RUNTIME%\"

@ECHO == Copy Orleans Client Binaries
set D="%ORL_CLIENT_BINARIES%"
if not exist "%D%" (md "%D%")
xcopy /y /s "%INDIR%\OrleansClient\*" "%ORL_CLIENT_BINARIES%\"

@ECHO == Copy Start Scripts and README to SDK root
copy /y "%INDIR%\Orleans-SDK\StartLocalSilo.cmd" "%OUTDIR%\"
copy /y "%INDIR%\Orleans-SDK\README.txt" "%OUTDIR%\"
copy /y "%INDIR%\Orleans-SDK\InstallOrleansVSTools*.cmd" "%OUTDIR%\"
copy /y "%INDIR%\Orleans-SDK\UninstallOrleansVSTools*.cmd" "%OUTDIR%\"

@ECHO == Copy docs
set D="%DOCSDIR%"
if not exist "%D%" (md "%D%")
copy /y "%INDIR%\Orleans-SDK\OrleansHelp\*.chm" "%D%\"
@REM copy /y "%INDIR%\Orleans-SDK\*.docx" "%D%\"
@REM copy /y "%INDIR%\Orleans-SDK\*.rtf" "%D%\"
@REM copy /y "%INDIR%\Orleans-SDK\*.pdf" "%D%\"
copy /y "%INDIR%\Orleans-SDK\*.mht" "%D%\"
copy /y "%INDIR%\Orleans-SDK\*.txt" "%D%\"


@ECHO == Copy VS Templates
set D="%VS_TEMPLATES%"
if not exist "%D%" (md "%D%")
xcopy /y "%INDIR%\Orleans-SDK\*.vsix" "%VS_TEMPLATES%\"

@ECHO == Copy Remote Deployment config files and scripts
set D="%DEPLOYCFGDIR%"
if not exist "%D%" (md "%D%")
xcopy /y /s "%INDIR%\Orleans-SDK\RemoteDeployment\*" "%DEPLOYCFGDIR%\"
copy /y "%INDIR%\Scripts\*.ps1" "%DEPLOYCFGDIR%\"
if exist "%DEPLOYCFGDIR%\AnalyzeOrleansSiloLogs.ps1" (del /q "%DEPLOYCFGDIR%\AnalyzeOrleansSiloLogs.ps1")

@ECHO == Copy Sample binaries - Hello2012
set APPNAME=Hello2012
@REM set D="%APPDIR%\%APPNAME%"
@REM if not exist "%D%" (md "%D%")
@REM xcopy /y /s "%INDIR%\Applications\%APPNAME%\*" "%D%\"
@REM set D="%APPBINDIR%\%APPNAME%"
@REM if not exist "%D%" (md "%D%")
@REM xcopy /y /s "%INDIR%\Applications\%APPNAME%\*" "%D%\"
set D="%SAMPLESOUT%\%APPNAME%"
if not exist "%D%" (md "%D%")
xcopy /y /s "%SAMPLESRC%\%APPNAME%\*" "%D%\"
xcopy /y /s "%SAMPLESRC%\%APPNAME%\*" "%D%\"

@ECHO == Copy Sample - Chirper2012
set APPNAME=Chirper2012
@REM @ECHO == Copy Sample binaries - %APPNAME% server
@REM set D=%APPDIR%\%APPNAME%
@REM if not exist "%D%" (md "%D%")
@REM xcopy /y /s "%INDIR%\Applications\%APPNAME%\*" "%D%\"
@REM set D=%APPBINDIR%\%APPNAME%
@REM if not exist "%D%" (md "%D%")
@REM xcopy /y /s "%INDIR%\Applications\%APPNAME%\*" "%D%\"
@ECHO == Copy Sample binaries - %APPNAME% client
@REM set D=%OUTDIR%\Binaries\%APPNAME%Client
@REM if not exist "%D%" (md "%D%")
@REM xcopy /y /s "%INDIR%\%APPNAME%Client\*" "%D%\"
@ECHO == Copy Sample source code - %APPNAME%
set D=%SAMPLESOUT%\%APPNAME%
if not exist "%D%" (md "%D%")
@ECHO xcopy /y /s "%SAMPLESRC%\%APPNAME%\*" "%D%\"
xcopy /y /s "%SAMPLESRC%\%APPNAME%\*" "%D%\"

@ECHO == Copy Sample - Presence
set APPNAME=Presence
@REM @ECHO == Copy Sample binaries - %APPNAME% server
@REM set D=%APPDIR%\%APPNAME%
@REM if not exist "%D%" (md "%D%")
@REM xcopy /y /s "%INDIR%\Applications\%APPNAME%\*" "%D%\"
@REM set D=%APPBINDIR%\%APPNAME%
@REM if not exist "%D%" (md "%D%")
@REM xcopy /y /s "%INDIR%\Applications\%APPNAME%\*" "%D%\"
@REM @ECHO == Copy Sample binaries - %APPNAME% client
@REM set D=%OUTDIR%\Binaries\%APPNAME%Client
@REM if not exist "%D%" (md "%D%")
@REM xcopy /y /s "%INDIR%\%APPNAME%Client\*" "%D%\"
@ECHO == Copy Sample source code - %APPNAME%
set D=%SAMPLESOUT%\%APPNAME%
if not exist "%D%" (md "%D%")
@ECHO xcopy /y /s "%SAMPLESRC%\%APPNAME%\*" "%D%\"
xcopy /y /s "%SAMPLESRC%\%APPNAME%\*" "%D%\"

@ECHO == Copy Sample - AzureWebSample
set APPNAME=AzureWebSample
set D=%SAMPLESOUT%\%APPNAME%
if not exist "%D%" (md "%D%")
@ECHO xcopy /y /s "%SAMPLESRC%\%APPNAME%\*" "%D%\"
xcopy /y /s "%SAMPLESRC%\%APPNAME%\*" "%D%\"

@ECHO == Copy References
set APPNAME=References
set D="%SAMPLESOUT%\%APPNAME%"
if not exist "%D%" (md "%D%")
xcopy /y /s "%SAMPLESRC%\%APPNAME%\*" "%D%\"

@ECHO == Delete obj and bin directories from samples
for /r "%SAMPLESOUT%" %%i in (.) do if exist "%%i\bin" (rd /s /q "%%i\bin")
for /r "%SAMPLESOUT%" %%i in (.) do if exist "%%i\obj" (rd /s /q "%%i\obj")
for /r "%SAMPLESOUT%" %%i in (.) do if exist "%%i\.nuget" (rd /s /q "%%i\.nuget")

@Echo == Strip Source Control Information from Project and Solution Files
"%CMDHOME%\StripSourceControl.exe" "%SAMPLESOUT%" "*.csproj, *.ccproj"

@ECHO == Copying SDK binaries for signing
if not exist "%OUTDIR%\..\ForCodesign" (md "%OUTDIR%\..\ForCodesign")
@ECHO == Copying dlls for signing
for /r "%OUTDIR%" %%i in (*.dll) do copy /y "%%i" "%OUTDIR%\..\ForCodesign\"
@ECHO == Copying exes for signing
for /r "%OUTDIR%" %%i in (*.exe) do copy /y "%%i" "%OUTDIR%\..\ForCodesign\"
@ECHO == Removing dependency binaries
del /q "%OUTDIR%\..\ForCodesign\M*"
del /q "%OUTDIR%\..\ForCodesign\IL*"
