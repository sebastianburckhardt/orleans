@setlocal
@echo off
@if NOT "%ECHO%"=="" @echo %ECHO%

set CMDHOME=%~dp0.

set DEPLOY_HOME=%CMDHOME%\RemoteDeployment
set MANAGER_BIN=%CMDHOME%\Binaries\OrleansServer
set MANAGER_EXE=%MANAGER_BIN%\OrleansManager.exe

set MANAGER_PARAMS=
set MANAGER_PARAMS=%MANAGER_PARAMS% Deployment.xml

@echo == Starting Orleans Manager in %DEPLOY_HOME%

start "Orleans.Manager" /D "%DEPLOY_HOME%" "%MANAGER_EXE%" %MANAGER_PARAMS% %*