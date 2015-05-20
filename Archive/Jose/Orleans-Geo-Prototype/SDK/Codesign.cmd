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
  set DSTSDK=%1\..\SDK-Signed
) else (
  set DSTSDK=%2
)

set CMDHOME=%~dp0.

set SRCSDK=%SRCDIR%
set SIGNEDDIR=%SRCDIR%\..\Signed

if not exist "%SRCSDK%" (
  @ECHO Cannot find %SRCSDK%
  GOTO END
)

if not exist "%SIGNEDDIR%" (
  @ECHO Cannot find %SIGNEDDIR%
  GOTO END
)

if not exist "%DSTSDK%" (md "%DSTSDK%")
xcopy /s /y "%SRCSDK%" "%DSTSDK%\"

@echo Replacing dll files with signed copies...
for /r "%DSTSDK%" %%i in (*.dll,*.exe) do (
  if exist "%SIGNEDDIR%\%%~ni%%~xi" (
    @echo Replacing %%i with %SIGNEDDIR%\%%~ni%%~xi
	copy /y "%SIGNEDDIR%\%%~ni%%~xi" "%%i"
  )
)

:END