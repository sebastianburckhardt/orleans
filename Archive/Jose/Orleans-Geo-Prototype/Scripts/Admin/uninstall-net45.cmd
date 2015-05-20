@setlocal
@echo off
@IF NOT "%ECHO%"=="" @echo %ECHO%

set CMDHOME=%~dp0.

set CMDNAME=Uninstall-Net40.cmd

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

set SHARE=\\17xcg1801\Shared

"%CMDHOME%\PsExec.exe" @%FNAME% -u %USERDOMAIN%\%USERNAME% -e -h %SHARE%\dotnetfx45_full_x86_x64.exe /q /uninstall
