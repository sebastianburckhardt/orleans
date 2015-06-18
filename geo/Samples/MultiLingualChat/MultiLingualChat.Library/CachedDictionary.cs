// ----------------------------------------------------------------------------------
// Microsoft Developer & Platform Evangelism
// 
// Copyright (c) Microsoft Corporation. All rights reserved.
// 
// THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, 
// EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES 
// OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
// ----------------------------------------------------------------------------------
// The example companies, organizations, products, domain names,
// e-mail addresses, logos, people, places, and events depicted
// herein are fictitious.  No association with any real company,
// organization, product, domain name, email address, logo, person,
// places, or events is intended or should be inferred.
// ----------------------------------------------------------------------------------

// <copyright file="CachedDictionary.cs" company="open-source" >
//  Copyright binary (c) 2012  by Haishi Bai
//   
//  Redistribution and use in source and binary forms, with or without modification, are permitted.
//
//  The names of its contributors may not be used to endorse or promote products derived from this software without specific prior written permission.
//
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// </copyright>

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using Microsoft.ApplicationServer.Caching;

namespace MultiLingualChat.Library
{
    public class CachedDictionary<T> : IDictionary<string, T>
    {
        private Dictionary<string, T> mCache; // aaasz: Replaced DataCache with local dictionary 

        public string Name { get; private set; }
        #region Constructors
        public CachedDictionary()
            : this(Guid.NewGuid().ToString("N"))
        {
        }
        public CachedDictionary(string name)
        {
            Name = name;
            //System.Diagnostics.Debug.WriteLine("Creating cache dictionary: " + name);
            mCache = new Dictionary<string, T>();
            //mCache.CreateRegion(Name);
            //System.Diagnostics.Debug.WriteLine("Cached dictionary created: " + name);
        }
        #endregion
        public void Add(string key, T value)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException("key", "item key can't be null.");
            if (!(value is T))
                throw new ArgumentException("item type mismatch.");
            mCache[key] = value;
        }
        public bool ContainsKey(string key)
        {
            return mCache.ContainsKey(key);
        }
        public ICollection<string> Keys
        {
            get
            {
                return getKeys();
            }
        }
        private ReadOnlyCollection<string> getKeys()
        {
            Dictionary<string, T>.KeyCollection list = mCache.Keys;
            List<string> keys = new List<string>();
            foreach (string item in list)
                keys.Add(item);
            return new ReadOnlyCollection<string>(keys);
            //return new ReadOnlyCollection<string>(new CollectionListWrapper<string>(mCache.Keys));
        }

        public bool Remove(string key)
        {
            return mCache.Remove(key);
        }

        public bool TryGetValue(string key, out T value)
        {
            //TODO: should this be an exception, or returning false?
            //if (string.IsNullOrEmpty(key))
            //    throw new ArgumentNullException("key", "item key can't be null.");

            return mCache.TryGetValue(key, out value);
        }
        public ICollection<T> Values
        {
            get { return getValues(); }
        }
        private ReadOnlyCollection<T> getValues()
        {
            Dictionary<string, T>.ValueCollection list = mCache.Values;
            List<T> values = new List<T>();
            foreach (T item in list)
                values.Add(item);
            return new ReadOnlyCollection<T>(values);
        }   
        
        public T this[string key]
        {
            get
            {
                T value;
                if (TryGetValue(key, out value))
                    return value;
                throw new ArgumentException("item not found.");
            }
            set
            {
                System.Diagnostics.Debug.WriteLine("Adding to key:" + key + " value: " + value);
                mCache[key] = value;
            }
        }
        public void Add(KeyValuePair<string, T> item)
        {
            Add(item.Key, item.Value);
        }
        public void Clear()
        {
            mCache.Clear();
        }
        public bool Contains(KeyValuePair<string, T> item)
        {
            T value;
            return TryGetValue(item.Key, out value);
        }
        public int Count
        {
            get
            {
                return getKeys().Count;
            }
        }
        public bool IsReadOnly
        {
            get { return false; } //TODO: examine this.
        }
        public bool Remove(KeyValuePair<string, T> item)
        {
            return Remove(item.Key);
        }
        
        IEnumerator IEnumerable.GetEnumerator()
        {
            return mCache.GetEnumerator();
        }


        public void CopyTo(KeyValuePair<string, T>[] array, int arrayIndex)
        {
            mCache.ToArray();
        }

        IEnumerator<KeyValuePair<string, T>> IEnumerable<KeyValuePair<string, T>>.GetEnumerator()
        {
            return mCache.GetEnumerator();
        }
    }
}
