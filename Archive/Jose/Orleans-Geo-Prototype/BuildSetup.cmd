@setlocal
@echo off
@if NOT "%ECHO%"=="" @echo %ECHO%

if "%1"=="" (
  @ECHO Need to specify target path
  goto END
) else (
  set SRCDIR=%1
)
if "%2"=="" (
  @ECHO Need to specify target configuration: DEBUG | RELEASE | SIGNED
  goto END
) else (
  set CONFIG=%2
)

msbuild /p:BinariesRoot=%SRCDIR%\;Configuration=%CONFIG%;OutDir=%SRCDIR%\%CONFIG%\ Setup\OrleansSetup.wixproj

:END