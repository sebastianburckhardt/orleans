@setlocal
@echo off
@if NOT "%ECHO%"=="" @echo %ECHO%

if "%1"=="" (
  @ECHO Need to specify MSI file
  goto END
) else (
  set SRCMSI=%1
)
if "%2"=="" (
  @ECHO Need to specify destination folder
  goto END
) else (
  set DSTDIR=%2
  set FILESDIR=%2\Files
)

set CMDHOME=%~dp0.

if not exist "%DSTDIR%" md "%DSTDIR%"
if not exist "%FILESDIR%" md "%FILESDIR%"

pushd "%DSTDIR%"
msidb.exe -d "%SRCMSI%" -x media1.cab
expand "%DSTDIR%\media1.cab" -F:* "%FILESDIR%"\
popd

@ECHO Done.
:END