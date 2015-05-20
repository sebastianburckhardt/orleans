set CMDHOME=%~dp0

C:
cd "C:\Program Files (x86)\Microsoft Visual Studio 11.0\VC"
call vcvarsall.bat x86

E:
cd "%CMDHOME%"
call build.cmd /p:BuildNumber=1 /p:RunTest=false 


set SRC=E:\Depot\Orleans\Code\Main\Test\PerfTests\Gabi - Config files for 17xcg container
set DST=E:\Depot\Orleans\Code\bin\SDK-DROP\RemoteDeployment
xcopy /y  "%SRC%\Deployment.xml"  		"%DST%"
xcopy /y  "%SRC%\OrleansConfiguration.xml"  	"%DST%"
xcopy /y  "%SRC%\ClientConfiguration.xml"  	"%DST%"


pause

