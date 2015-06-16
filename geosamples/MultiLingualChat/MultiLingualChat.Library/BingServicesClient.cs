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

// <copyright file="BingServiceClient.cs" company="open-source" >
//  Copyright binary (c) 2012  by Haishi Bai
//   
//  Redistribution and use in source and binary forms, with or without modification, are permitted.
//
//  The names of its contributors may not be used to endorse or promote products derived from this software without specific prior written permission.
//
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// </copyright>

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Xml;
using System.Xml.Linq;
using Microsoft.ApplicationServer.Caching;
using MultiLingualChat.Entities;

namespace MultiLingualChat.Library
{
    public class BingServicesClient
    {
        //private static string appId = Microsoft.WindowsAzure.CloudConfigurationManager.GetSetting("BingAppId");
        private static string appId = "10";
        public static string Translate(string text, string originalLanguage, string targetLanguage)
        {
            try
            {
                string uri = "http://api.microsofttranslator.com/v2/Http.svc/Translate?appId=" + appId + "&text=" + HttpUtility.UrlEncode(text) + "&from=" + originalLanguage + "&to=" + targetLanguage;
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);
                WebResponse response = request.GetResponse();
                using (Stream strm = response.GetResponseStream())
                {
                    XElement elm = XElement.Load(strm);
                    var ret = elm.FirstNode.ToString();
                    return ret;
                }
            }
            catch (Exception exp)
            {
                Trace.WriteLine(exp.ToString());
                return text;
            }
        }
        public static List<string> GetLanguagesForTranslate()
        {
            string uri = "http://api.microsofttranslator.com/v2/Http.svc/GetLanguagesForTranslate?appId=" + appId;
            HttpWebRequest httpWebRequest = (HttpWebRequest)WebRequest.Create(uri);
            WebResponse response = null;
            try
            {
                response = httpWebRequest.GetResponse();
                using (Stream stream = response.GetResponseStream())
                {
                    System.Runtime.Serialization.DataContractSerializer dcs = new System.Runtime.Serialization.DataContractSerializer(typeof(List<string>));
                    return (List<string>)dcs.ReadObject(stream);
                }
            }
            catch (WebException)
            {
                return new List<string>(new string[]{"en"});
            }
            catch
            {
                throw;
            }
            finally
            {
                if (response != null)
                {
                    response.Close();
                    response = null;
                }
            }
        }
        public static string[] GetLanguageNames(string[] languageCodes)
        {
            string uri = "http://api.microsofttranslator.com/v2/Http.svc/GetLanguageNames?locale=" + languageCodes[0] + "&appId=" + appId;
            HttpWebRequest httpWebRequest = (HttpWebRequest)WebRequest.Create(uri);
            httpWebRequest.ContentType = "text/xml";
            httpWebRequest.Method = "POST";
            System.Runtime.Serialization.DataContractSerializer dcs = new System.Runtime.Serialization.DataContractSerializer(Type.GetType("System.String[]"));
            using (System.IO.Stream stream = httpWebRequest.GetRequestStream())
            {
                dcs.WriteObject(stream, languageCodes);
            }

            WebResponse response = null;
            try
            {
                response = httpWebRequest.GetResponse();
                using (Stream stream = response.GetResponseStream())
                {
                    return (string[])dcs.ReadObject(stream);
                }
            }
            catch (WebException)
            {
                if (languageCodes.Length == 1 && languageCodes[0] == "en")
                    return new string[] { "English" };
                else
                    throw;
            }
            catch
            {
                throw;
            }
            finally
            {
                if (response != null)
                {
                    response.Close();
                    response = null;
                }
            }
        }
        public static LanguageEntity[] GetLanguages()
        {
            List<string> languages = GetLanguagesForTranslate();
            var mLanguages = new LanguageEntity[languages.Count];
            System.Diagnostics.Debug.WriteLine("Number of languages: " + languages.Count);
            for (int i = 0; i < languages.Count; i++)
            {
                string[] languageNames = GetLanguageNames(new string[] { languages[i] });
                mLanguages[i] = new LanguageEntity(languages[i], languageNames[0]);
            }
            return mLanguages;
        }

        public static List<string> SearchImage(string key)
        {
            List<string> ret = new List<string>();
            try
            {
                string uri = "http://api.bing.net/xml.aspx?Appid=" + appId + "&query=" + HttpUtility.UrlEncode(key) + "&sources=image&Image.Filters=Size:Small";
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);
                WebResponse response = request.GetResponse();
                using (Stream strm = response.GetResponseStream())
                {
                    XElement elm = XElement.Load(strm);
                    var list = from node in elm.Descendants(XName.Get("MediaUrl", "http://schemas.microsoft.com/LiveSearch/2008/04/XML/multimedia"))
                               select node;
                    foreach(var node in list)
                        ret.Add(node.Value);
                }
            }
            catch (Exception exp)
            {
                Trace.WriteLine(exp.ToString());
            }
            return ret;
        }
    }
}
