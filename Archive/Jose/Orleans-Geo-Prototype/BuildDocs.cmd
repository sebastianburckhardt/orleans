@setlocal

@REM NOTE: This script must be run from a Visual Studio 2010 command prompt window

SET MSBUILDVER=v4.0.30319
SET MSBUILDEXE=%FrameworkDir%%MSBUILDVER%\MSBuild.exe
SET builduri=local

%MSBUILDEXE% BuildDefinitions\TFSBuildDocs.proj 

