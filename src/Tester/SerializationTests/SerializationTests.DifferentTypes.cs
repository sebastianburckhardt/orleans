/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/


using System;
using System.Globalization;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Serialization;
using System.Collections.Generic;
using Orleans.TestingHost;

namespace UnitTests.Serialization
{
    /// <summary>
    /// Summary description for SerializationTests
    /// </summary>
    [TestClass]
    public class SerializationTestsDifferentTypes
    {
        [TestInitialize]
        public void InitializeForTesting()
        {
            SerializationManager.InitializeForTesting();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Serialization")]
        public void SerializationTests_DateTime()
        {
            // Local Kind
            DateTime inputLocal = DateTime.Now;

            DateTime outputLocal = SerializationManager.RoundTripSerializationForTesting(inputLocal);
            Assert.AreEqual(inputLocal.ToString(CultureInfo.InvariantCulture), outputLocal.ToString(CultureInfo.InvariantCulture));
            Assert.AreEqual(inputLocal.Kind, outputLocal.Kind);

            // UTC Kind
            DateTime inputUtc = DateTime.UtcNow;

            DateTime outputUtc = SerializationManager.RoundTripSerializationForTesting(inputUtc);
            Assert.AreEqual(inputUtc.ToString(CultureInfo.InvariantCulture), outputUtc.ToString(CultureInfo.InvariantCulture));
            Assert.AreEqual(inputUtc.Kind, outputUtc.Kind);

            // Unspecified Kind
            DateTime inputUnspecified = new DateTime(0x08d27e2c0cc7dfb9);

            DateTime outputUnspecified = SerializationManager.RoundTripSerializationForTesting(inputUnspecified);
            Assert.AreEqual(inputUnspecified.ToString(CultureInfo.InvariantCulture), outputUnspecified.ToString(CultureInfo.InvariantCulture));
            Assert.AreEqual(inputUnspecified.Kind, outputUnspecified.Kind);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Serialization")]
        public void SerializationTests_DateTimeOffset()
        {
            // Local Kind
            DateTime inputLocalDateTime = DateTime.Now;
            DateTimeOffset inputLocal = new DateTimeOffset(inputLocalDateTime);

            DateTimeOffset outputLocal = SerializationManager.RoundTripSerializationForTesting(inputLocal);
            Assert.AreEqual(inputLocal, outputLocal, "Local time");
            Assert.AreEqual(
                inputLocal.ToString(CultureInfo.InvariantCulture),
                outputLocal.ToString(CultureInfo.InvariantCulture));
            Assert.AreEqual(inputLocal.DateTime.Kind, outputLocal.DateTime.Kind);

            // UTC Kind
            DateTime inputUtcDateTime = DateTime.UtcNow;
            DateTimeOffset inputUtc = new DateTimeOffset(inputUtcDateTime);

            DateTimeOffset outputUtc = SerializationManager.RoundTripSerializationForTesting(inputUtc);
            Assert.AreEqual(inputUtc, outputUtc, "UTC time");
            Assert.AreEqual(
                inputUtc.ToString(CultureInfo.InvariantCulture),
                outputUtc.ToString(CultureInfo.InvariantCulture));
            Assert.AreEqual(inputUtc.DateTime.Kind, outputUtc.DateTime.Kind);

            // Unspecified Kind
            DateTime inputUnspecifiedDateTime = new DateTime(0x08d27e2c0cc7dfb9);
            DateTimeOffset inputUnspecified = new DateTimeOffset(inputUnspecifiedDateTime);

            DateTimeOffset outputUnspecified = SerializationManager.RoundTripSerializationForTesting(inputUnspecified);
            Assert.AreEqual(inputUnspecified, outputUnspecified, "Unspecified time");
            Assert.AreEqual(
                inputUnspecified.ToString(CultureInfo.InvariantCulture),
                outputUnspecified.ToString(CultureInfo.InvariantCulture));
            Assert.AreEqual(inputUnspecified.DateTime.Kind, outputUnspecified.DateTime.Kind);
        }

        [Serializable]
        internal class TestTypeA
        {
            public ICollection<TestTypeA> Collection { get; set; }
        }

        [global::Orleans.CodeGeneration.RegisterSerializerAttribute()]
        internal class TestTypeASerialization
        {

            static TestTypeASerialization()
            {
                Register();
            }

            public static object DeepCopier(object original)
            {
                TestTypeA input = ((TestTypeA)(original));
                TestTypeA result = new TestTypeA();
                Orleans.Serialization.SerializationContext.Current.RecordObject(original, result);
                result.Collection = ((System.Collections.Generic.ICollection<TestTypeA>)(Orleans.Serialization.SerializationManager.DeepCopyInner(input.Collection)));
                return result;
            }

            public static void Serializer(object untypedInput, Orleans.Serialization.BinaryTokenStreamWriter stream, System.Type expected)
            {
                TestTypeA input = ((TestTypeA)(untypedInput));
                Orleans.Serialization.SerializationManager.SerializeInner(input.Collection, stream, typeof(System.Collections.Generic.ICollection<TestTypeA>));
            }

            public static object Deserializer(System.Type expected, global::Orleans.Serialization.BinaryTokenStreamReader stream)
            {
                TestTypeA result = new TestTypeA();
                DeserializationContext.Current.RecordObject(result);
                result.Collection = ((System.Collections.Generic.ICollection<TestTypeA>)(Orleans.Serialization.SerializationManager.DeserializeInner(typeof(System.Collections.Generic.ICollection<TestTypeA>), stream)));
                return result;
            }

            public static void Register()
            {
                global::Orleans.Serialization.SerializationManager.Register(typeof(TestTypeA), DeepCopier, Serializer, Deserializer);
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Serialization")]
        public void SerializationTests_RecursiveSerialization()
        {
            TestTypeA input = new TestTypeA();
            input.Collection = new HashSet<TestTypeA>();
            input.Collection.Add(input);

            TestTypeA output1 = TestingUtils.RoundTripDotNetSerializer(input);

            TestTypeA output2 = SerializationManager.RoundTripSerializationForTesting(input);
        }
    }
}
