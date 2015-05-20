// UnitTestCLIGrains.h

#pragma once

using namespace System;
using namespace Orleans;
using namespace SimpleGrain;


namespace UnitTestCLIGrains {

	public ref class SimpleCLIGrain : ISimpleCLIGrain, GrainBase
	{
		// TODO: Add your methods for this class here.

		public:
		virtual Task^ SetA(int a);
		virtual AsyncCompletion^ SetB(int a);
		virtual AsyncCompletion^ IncrementA();
		virtual AsyncValue<int>^ GetAxB();
		virtual AsyncValue<int>^ GetAxB(int a, int b);
		virtual AsyncValue<int>^ GetA();
		virtual AsyncCompletion^ ReadOnlyInterlock(int timeout);
		virtual AsyncCompletion^ ExclusiveWait(int timeout);
		virtual AsyncCompletion^ Subscribe(ISimpleGrainObserver^ observer);
		virtual AsyncCompletion^ Unsubscribe(ISimpleGrainObserver^ observer);

		virtual property Orleans::AsyncValue<int>^ A
		{
				Orleans::AsyncValue<int>^ get();
		};
		
		private:
		int _a;
		int _b;
	};
}
