@setlocal
@echo off
@if NOT "%ECHO%"=="" @echo %ECHO%

set CMDHOME=%~dp0

@echo Deleting build output files
For /D %%a in ("%CMDHOME%*") Do (
if exist "%%a\bin\Debug" (
echo Deleting files in %%a\bin\Debug
rd /S/Q "%%a\bin\Debug"
)
if exist "%%a\obj\Debug" (
echo Deleting files in %%a\obj\Debug
rd /S/Q "%%a\obj\Debug"
)
if exist "%%a\obj\x86\Debug" (
echo Deleting files in %%a\obj\x86\Debug
rd /S/Q "%%a\obj\x86\Debug"
)
if exist "%%a\obj\x64\Debug" (
echo Deleting files in %%a\obj\x64\Debug
rd /S/Q "%%a\obj\x64\Debug"
)

if exist "%%a\bin\Release" (
echo Deleting files in %%a\bin\Release
rd /S/Q "%%a\bin\Release"
)
if exist "%%a\obj\Release" (
echo Deleting files in %%a\obj\Release
rd /S/Q "%%a\obj\Release"
)
if exist "%%a\obj\x86\Release" (
echo Deleting files in %%a\obj\x86\Release
rd /S/Q "%%a\obj\x86\Release"
)
if exist "%%a\obj\x64\Release" (
echo Deleting files in %%a\obj\x64\Release
rd /S/Q "%%a\obj\x64\Release"
)
)


@echo  -----------

@echo Deleting files in TestInput
set TESTINPUT_DIR=%CMDHOME%.\TestInput
if exist "%TESTINPUT%" (
 rd /S/Q "%TESTINPUT%"
)

@echo Deleting persistent STORE
set D=%CMDHOME%.\STORE
if exist "%D%"  rd /S/Q "%D%"

@echo Deleting Deployment drop files...
For /D %%a in ("%CMDHOME%*Deployment") Do (
if exist "%%a\Debug" (
@echo Deleting deployment files in %%a
rd /S/Q "%%a\Debug"
)
if exist "%%a\Release" (
@echo Deleting deployment files in %%a
rd /S/Q "%%a\Release"
)
)

set D=%APPDATA%\..\Local\OrleansData\FactoryCache
@echo Deleting Orleans factory cache in %D%
if exist "%D%"  del /F/Q  "%D%\*"

@echo  -----------
@echo clean Orleans references
if exist "%CMDHOME%References" (
 rd /S/Q "%CMDHOME%References"
)
@echo clean samples references
if exist "%CMDHOME%..\Samples\References" (
 rd /S/Q "%CMDHOME%..\Samples\References"
)
@echo clean graph references
if exist "%CMDHOME%..\Graphs\DistributedGraph-Orleans-V2\References" (
 rd /S/Q "%CMDHOME%..\Graphs\DistributedGraph-Orleans-V2\References"
)
@echo - Running CleanAll.cmd for Graphs Projects
"%CMDHOME%..\Graphs\DistributedGraph-Orleans-V2\CleanAll.cmd"


