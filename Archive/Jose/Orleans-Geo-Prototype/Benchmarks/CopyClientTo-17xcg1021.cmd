del /q /s \\17xcg1021\c$\Users\gkliot\Documents\applications\BenchmarkTests
del /q /s C:\Depot\Orleans\Code\Prototype\OrleansV4\Test\bin\Debug\Client-*.log
del /q /s C:\Depot\Orleans\Code\Prototype\OrleansV4\Test\bin\Debug\Primary-*.log
del /q /s C:\Depot\Orleans\Code\Prototype\OrleansV4\Test\bin\Debug\Secondary*.log

ECHO ------------ COPY CLIENT ------------
 
xcopy /s /y  C:\Depot\Orleans\Code\Prototype\OrleansV4\Test\bin\Debug                                                        \\17xcg1021\c$\Users\gkliot\Documents\applications\BenchmarkTests
xcopy /s /y  C:\Depot\Orleans\Code\Prototype\OrleansV4\Benchmarks\Microsoft.VisualStudio.QualityTools.UnitTestFramework.dll  \\17xcg1021\c$\Users\gkliot\Documents\applications\BenchmarkTests
xcopy /s /y  "C:\Program Files (x86)\Reference Assemblies\Microsoft\FSharp\2.0\Runtime\v4.0\FSharp.Core.dll"                 \\17xcg1021\c$\Users\gkliot\Documents\applications\BenchmarkTests

xcopy /s /y  C:\Depot\Orleans\Code\Prototype\OrleansV4\Benchmarks\1.runExchangeMessage.bat                                   \\17xcg1021\c$\Users\gkliot\Documents\applications\BenchmarkTests
xcopy /s /y  C:\Depot\Orleans\Code\Prototype\OrleansV4\Benchmarks\1.runExchangeMessage-Processes.bat                         \\17xcg1021\c$\Users\gkliot\Documents\applications\BenchmarkTests
xcopy /s /y  C:\Depot\Orleans\Code\Prototype\OrleansV4\Benchmarks\StartOrleansPrimary.bat                                    \\17xcg1021\c$\Users\gkliot\Documents\applications\BenchmarkTests
xcopy /s /y  C:\Depot\Orleans\Code\Prototype\OrleansV4\Benchmarks\StartOrleansNode2.bat                                      \\17xcg1021\c$\Users\gkliot\Documents\applications\BenchmarkTests



pause
