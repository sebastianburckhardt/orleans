@setlocal

@REM NOTE: This script must be run from a Visual Studio 2010 command prompt window

SET MSBUILDVER=v4.0.30319
SET MSBUILDEXE=%FrameworkDir%%MSBUILDVER%\MSBuild.exe
SET MSBuildForwardPropertiesFromChild=local

%MSBUILDEXE% BuildDefinitions\TFSBuild.proj %* /p:RunTests=false /p:RunTest=false /p:BuildNumber=1
pushd ..\Test\LoadTests\
%MSBUILDEXE% LoadTests.sln %*
mkdir TestInput
mstest /testmetadata:LoadTests1.vsmdi /testlist:LocalNightly