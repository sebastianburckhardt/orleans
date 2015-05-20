@setlocal
@REM @echo off
@if NOT "%ECHO%"=="" @echo %ECHO%

set CMDHOME=%~dp0.

@ECHO -- Start BuildPkg.cmd - %CMDHOME%

if "%2"=="" (
  set INDIR=.
) else (
  set INDIR=%2
)
if "%1"=="" (
  set OUTDIR=C:\Orleans
) else (
  set OUTDIR=%1
)

set RUNTIMEDIR=%OUTDIR%\Orleans
set SDKDIR=%OUTDIR%\Orleans-SDK
set APPDIR=%OUTDIR%\Applications
set CLIENTDIR=%OUTDIR%\OrleansClient
@ECHO set WSPSETUP=%OUTDIR%\WspEventRouterSetup
set DEPLOYCFGDIR=%SDKDIR%\RemoteDeployment
set SAMPLESDIR=%OUTDIR%\Samples
set SCRIPTSDIR=%OUTDIR%\Scripts

set AZURE_SDK_VERSION=2.2

@echo == Building Orleans deployment package to %OUTDIR%

set SNK=%CMDHOME%\Orleans.snk

@echo InDir=%INDIR%
@echo OutDir=%OUTDIR%
@echo AppDir=%APPDIR%
@echo SDKDir=%SDKDIR%
@echo SNK=%SNK%

if not exist "%OUTDIR%" (md "%OUTDIR%")
if not exist "%RUNTIMEDIR%" (md "%RUNTIMEDIR%")
if not exist "%RUNTIMEDIR%\Applications" (md "%RUNTIMEDIR%\Applications")
if not exist "%APPDIR%" (md "%APPDIR%")
if not exist "%SDKDIR%" (md "%SDKDIR%")
if not exist "%DEPLOYCFGDIR%" (md "%DEPLOYCFGDIR%")
if not exist "%CLIENTDIR%" (md "%CLIENTDIR%")
if not exist "%SCRIPTSDIR%" (md "%SCRIPTSDIR%")

@ECHO == Copy config files
copy /y "%INDIR%\Deployment.xml" "%RUNTIMEDIR%\"
copy /y "%INDIR%\OrleansConfiguration.xml" "%RUNTIMEDIR%\"
copy /y "%INDIR%\ClientConfiguration.xml" "%CLIENTDIR%\"

@ECHO == Copy exe's
copy /y "%INDIR%\CounterControl.*" "%RUNTIMEDIR%\"
copy /y "%INDIR%\OrleansHost.*" "%RUNTIMEDIR%\"
copy /y "%INDIR%\OrleansManager.*" "%RUNTIMEDIR%\"
copy /y "%INDIR%\OrleansManager.*" "%CLIENTDIR%\"
copy /y "%INDIR%\ClientGenerator.*" "%RUNTIMEDIR%\"
copy /y "%INDIR%\ClientGenerator.*" "%CLIENTDIR%\"
copy /y "%CMDHOME%\Dependencies\ILMerge\ILMerge.exe*" "%RUNTIMEDIR%\"
copy /y "%CMDHOME%\Dependencies\ILMerge\ILMerge.exe*" "%CLIENTDIR%\"
copy /y "%CMDHOME%\Dependencies\Windows-Azure-SDK\v%AZURE_SDK_VERSION%\ref\Microsoft.WindowsAzure.StorageClient.dll" "%RUNTIMEDIR%\"
copy /y "%CMDHOME%\Dependencies\Windows-Azure-SDK\v%AZURE_SDK_VERSION%\ref\Microsoft.WindowsAzure.StorageClient.dll" "%CLIENTDIR%\"
copy /y "%CMDHOME%\Dependencies\Windows-Azure-SDK\v%AZURE_SDK_VERSION%\ref\Microsoft.WindowsAzure.Configuration.dll" "%RUNTIMEDIR%\"
copy /y "%CMDHOME%\Dependencies\Windows-Azure-SDK\v%AZURE_SDK_VERSION%\ref\Microsoft.WindowsAzure.Configuration.dll" "%CLIENTDIR%\"
copy /y "%CMDHOME%\Dependencies\Newtonsoft.Json\Json.Net-50r8\Bin\Net45\Newtonsoft.Json.dll" "%RUNTIMEDIR%\"
copy /y "%INDIR%\ClientGen.cmd" "%RUNTIMEDIR%\"
copy /y "%INDIR%\ClientGen.cmd" "%CLIENTDIR%\"

@ECHO == Copy MsBuild files
copy /y "%CMDHOME%\Orleans.SDK.targets" "%CLIENTDIR%\"
copy /y "%CMDHOME%\Orleans.SDK.targets" "%RUNTIMEDIR%\"
copy /y "%INDIR%\OrleansMsBuildTask.*" "%CLIENTDIR%\"
copy /y "%INDIR%\OrleansMsBuildTask.*" "%RUNTIMEDIR%\"

@ECHO == Copy server DLLs
copy /y "%INDIR%\OrleansRuntime*" "%RUNTIMEDIR%\"
copy /y "%INDIR%\OrleansAzureUtils.*" "%RUNTIMEDIR%\"
copy /y "%INDIR%\OrleansProviders.*" "%RUNTIMEDIR%\"
copy /y "%INDIR%\OrleansProviderInterfaces.*" "%RUNTIMEDIR%\"
copy /y "%INDIR%\Orleans.*" "%RUNTIMEDIR%\"
copy /y "%INDIR%\Orleans.FSharp.*" "%RUNTIMEDIR%\"
del /q /f "%RUNTIMEDIR%\Orleans.Azure.Samples.WebRole.*"
del /q /f "%RUNTIMEDIR%\Orleans.Azure.Silos.WorkerRole.*"
del /q /f "%RUNTIMEDIR%\*.CodeAnalysisLog.xml"
del /q /f "%RUNTIMEDIR%\*.lastcodeanalysissucceeded"

@ECHO == Copy dependencies
copy /y "%INDIR%\Microsoft.WindowsAzure.*" "%RUNTIMEDIR%\"
copy /y "%CMDHOME%\Dependencies\Windows-Azure-SDK\v%AZURE_SDK_VERSION%\bin\runtimes\base\x64\msshrtmi.dll" "%RUNTIMEDIR%\"

del /q /f "%RUNTIMEDIR%\*.cs"
del /q /f "%RUNTIMEDIR%\*.Moles.dll"
del /q /f "%RUNTIMEDIR%\*.Moles.xml"

@ECHO == Copy scripts
copy /y "%INDIR%\StartOrleans.cmd" "%RUNTIMEDIR%\"
copy /y "%CMDHOME%\Scripts\Deployment\*.ps1" "%SCRIPTSDIR%\"

@ECHO == Copy client binaries
copy /y "%INDIR%\Orleans.*" "%CLIENTDIR%\"
copy /y "%INDIR%\Orleans.FSharp.*" "%CLIENTDIR%\"
copy /y "%INDIR%\OrleansAzureUtils.*" "%CLIENTDIR%\"
copy /y "%INDIR%\OrleansProviders.*"			"%CLIENTDIR%\"
copy /y "%INDIR%\OrleansProviderInterfaces.*"	"%CLIENTDIR%\"

del /q /f "%CLIENTDIR%\*.cs"
del /q /f "%CLIENTDIR%\*.Moles.dll"
del /q /f "%CLIENTDIR%\*.Moles.xml"
del /q /f "%CLIENTDIR%\Orleans.Deployment.*"
del /q /f "%CLIENTDIR%\Orleans.Management.Agents.*"
del /q /f "%CLIENTDIR%\Orleans.Azure.Samples.WebRole.*"
del /q /f "%CLIENTDIR%\Orleans.Azure.Silos.WorkerRole.*"
del /q /f "%CLIENTDIR%\*.CodeAnalysisLog.xml"
del /q /f "%CLIENTDIR%\*.lastcodeanalysissucceeded"

copy /y "%CMDHOME%\Build-Number.txt" "%RUNTIMEDIR%\"
copy /y "%CMDHOME%\Build-Number.txt" "%CLIENTDIR%\"

@ECHO == Copy Unit Test app
set D="%APPDIR%\UnitTestGrains"
if not exist "%D%" (md "%D%")
copy /y "%INDIR%\UnitTestGrains.*" "%D%\"
copy /y "%INDIR%\UnitTestGrainInterfaces.*" "%D%\"
copy /y "%INDIR%\UnsignedUnitTestGrains.*" "%D%\"
copy /y "%INDIR%\UnsignedUnitTestGrainInterfaces.*" "%D%\"

@ECHO == Copy XML schema files
copy /y "%INDIR%\RuntimeCore\Configuration\*.*" "%RUNTIMEDIR%\"
copy /y "%INDIR%\*.xsd" "%RUNTIMEDIR%\"

@ECHO == Copy SDK files
copy /y "%CMDHOME%\SDK\*" "%SDKDIR%\"
@REM copy /y "%CMDHOME%\Documentation\*.docx" "%SDKDIR%\"
@REM copy /y "%CMDHOME%\Documentation\*.rtf" "%SDKDIR%\"
@REM copy /y "%CMDHOME%\Documentation\*.pdf" "%SDKDIR%\"
copy /y "%CMDHOME%\Documentation\*.mht" "%SDKDIR%\"
copy /y "%CMDHOME%\Documentation\*.txt" "%SDKDIR%\"
@REM copy /y "%CMDHOME%\Documentation\*.html" "%SDKDIR%\"
copy /y "%CMDHOME%\Build-Number.txt" "%SDKDIR%\"
copy /y "%CMDHOME%\Version.txt" "%SDKDIR%\"
@REM copy /y "%INDIR%\*.docx" "%SDKDIR%\"

@REM The Sandcastle Help File Builder will put these in the same dir as the HTML help for each project.
@REM copy /y "%CMDHOME%\Documentation\Help\*.chm" "%SDKDIR%\"

xcopy /s/e/y "%CMDHOME%\Documentation\OrleansHelp\*" "%SDKDIR%\OrleansHelp\"
@REM xcopy /s/e/y "%CMDHOME%\Documentation\GraphHelp\*" "%SDKDIR%\GraphHelp\"

@ECHO == Copy Remote Deployment files
copy /y "%CMDHOME%\SDK\RemoteDeployment\*" "%DEPLOYCFGDIR%\"
@REM @ECHO == Copy sub-dirs required by UI
@REM set S=Skins
@REM set D="%DEPLOYCFGDIR%\%S%"
@REM if not exist "%D%" (md "%D%")
@REM copy /y "%INDIR%\%S%\*" "%D%\"

if not exist "%SAMPLESDIR%\References" (md "%SAMPLESDIR%\References")
xcopy /y /s "%CMDHOME%\\References\*" "%SAMPLESDIR%\References\"

@ECHO -- End BuildPkg.cmd - %CMDHOME%
