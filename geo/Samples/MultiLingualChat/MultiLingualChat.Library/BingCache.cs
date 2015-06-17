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

// <copyright file="TranslationRepository.cs" company="open-source" >
//  Copyright binary (c) 2012  by Haishi Bai
//   
//  Redistribution and use in source and binary forms, with or without modification, are permitted.
//
//  The names of its contributors may not be used to endorse or promote products derived from this software without specific prior written permission.
//
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// </copyright>

using MultiLingualChat.Entities;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Web;

namespace MultiLingualChat.Library
{
    public class BingCache
    {
        private Dictionary<string, Language[]> mLanguageCache = new Dictionary<string, Language[]>();

        public Language[] GetLanguages()
        {
            if (mLanguageCache.ContainsKey("ALL"))
                return mLanguageCache["ALL"];
            else
            {
                LanguageEntity[] list = new LanguageEntity[1];
                try
                {
                    list = BingServicesClient.GetLanguages();
                }
                catch (Exception e)
                {
                    System.Diagnostics.Debug.WriteLine(e);
                }
                var ret = from lan in list
                          select new Language { Code = lan.Code, Name = lan.Name };
                try
                {
                    mLanguageCache.Add("ALL", ret.ToArray());
                }
                catch (Exception exp)
                {
                    //it's ok for cache to fail
                    Trace.TraceError(exp.ToString());
                }
                return ret.ToArray();
            }
        }
        public string[] SearchImage(string name)
        {
            return BingServicesClient.SearchImage(name).ToArray();
        }
        public string Translate(string text, string originalLanguage, string targetLanguage)
        {
            return BingServicesClient.Translate(text, originalLanguage, targetLanguage);
        }

        public struct Language
        {
            public string Code { get; set; }
            public string Name { get; set; }
        }
    }
}