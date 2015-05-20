@setlocal
@echo off
@if NOT "%ECHO%"=="" @echo %ECHO%

if "%1"=="" (
  @ECHO Need to specify target path
  goto END
) else (
  set SRCDIR=%1
)

@echo Verifying signing of binaries in %SRCDIR% ...
for /r "%SRCDIR%" %%i in (*.dll,*.exe) do (
  @echo %%i
  sn -q -vf "%%i"
  sn -q -T "%%i"
)

:END