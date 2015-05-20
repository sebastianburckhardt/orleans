
cd C:\Depot\Orleans\Code\Prototype\OrleansV4\Test\bin\Debug


Test.exe -benchmark ExchangeMessage true 1 1000  100

Test.exe -benchmark ExchangeMessage true 1 1000  1000

Test.exe -benchmark ExchangeMessage true 1 1000  5000

Test.exe -benchmark ExchangeMessage true 1 1000  10000

Test.exe -benchmark ExchangeMessage true 1 1000  50000



Test.exe -benchmark ExchangeMessage true 2 1000  100

Test.exe -benchmark ExchangeMessage true 2 1000  1000

Test.exe -benchmark ExchangeMessage true 2 1000  5000

Test.exe -benchmark ExchangeMessage true 2 1000  10000

Test.exe -benchmark ExchangeMessage true 2 1000  50000

pause


rem Test.exe -benchmark create true 1