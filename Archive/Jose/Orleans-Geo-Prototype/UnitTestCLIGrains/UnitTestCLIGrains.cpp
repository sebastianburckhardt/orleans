// This is the main DLL file.

#include "stdafx.h"

#include "UnitTestCLIGrains.h"

namespace UnitTestCLIGrains {

	AsyncCompletion^ SimpleCLIGrain::SetA(int a)
	{
		_a = a;
		return AsyncCompletion::Done;
	}

	AsyncCompletion^ SimpleCLIGrain::SetB(int b)
	{
		_b = b;
		return AsyncCompletion::Done;
	}

	AsyncCompletion^ SimpleCLIGrain::IncrementA()
	{
		_a++;
		return AsyncCompletion::Done;
	}

	AsyncValue<int>^ SimpleCLIGrain::GetAxB()
	{
		return _a*_b;
	}
	
	AsyncValue<int>^ SimpleCLIGrain::GetAxB(int a, int b)
	{
		return a*b;
	}

	AsyncValue<int>^ SimpleCLIGrain::GetA()
	{
		return _a;
	}

	AsyncCompletion^ SimpleCLIGrain::ReadOnlyInterlock(int timeout)
	{
		return AsyncCompletion::Done;
	}
		
	AsyncCompletion^ SimpleCLIGrain::ExclusiveWait(int timeout)		
	{
		return AsyncCompletion::Done;
	}
	
	AsyncCompletion^ SimpleCLIGrain::Subscribe(ISimpleGrainObserver^ observer)
	{
		return AsyncCompletion::Done;
	}

	AsyncCompletion^ SimpleCLIGrain::Unsubscribe(ISimpleGrainObserver^ observer)
	{
		return AsyncCompletion::Done;
	}

	Orleans::AsyncValue<int>^ SimpleCLIGrain::A::get()
	{
		return _a;
	};
}