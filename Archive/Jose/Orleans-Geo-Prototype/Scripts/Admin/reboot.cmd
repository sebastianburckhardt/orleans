@setlocal
@echo off
@IF NOT "%ECHO%"=="" @echo %ECHO%

set CMDHOME=%~dp0.

set CMDNAME=Reboot.cmd

set FNAME=%1

IF EXIST "%FNAME%" (
  set HOSTS=@%FNAME%
) else (
  set HOSTS=%*
)

@ECHO -- Start %CMDNAME% - %HOSTS%

IF "%HOSTS%"=="" (
@ECHO ERROR - No hosts list or hosts file specified
EXIT /B 1
)

%CMDHOME%\PsExec.exe @%FNAME% -u %USERDOMAIN%\%USERNAME% -e -h shutdown /r
