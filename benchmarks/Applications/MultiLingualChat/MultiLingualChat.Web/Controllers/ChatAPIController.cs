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

// <copyright file="ChatAPIController.cs" company="open-source" >
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
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using MultiLingualChat.Entities;
using MultiLingualChat.Web.Models;
using System.Threading.Tasks;
using System.Text;
using MultiLingualChat.GrainInterfaces;
using System.Security.Cryptography;
using Orleans;
using MultiLingualChat.Library;

namespace MultiLingualChat.Web.Controllers
{
    public class ChatAPIController : ApiController
    {
        private MD5 md5Hash = MD5.Create();
        //private AlternativeTranslationRepository mRepo = RepositoryCreator.TableStorageInstance;
        //private RoomRepository mCache = RepositoryCreator.CacheInstance;
        
        private BingCache mBing = new BingCache();

        [HttpGet]
        public LanguageModel[] GetLanguages()
        {
            //System.Diagnostics.Debug.WriteLine("Called getLanguages");
            //System.Diagnostics.Debug.WriteLine("Available languages: " + mBing.GetLanguages());
            var languages = mBing.GetLanguages();
            var ret = from lan in languages
                      select new LanguageModel { Code = lan.Code, Name = lan.Name };

            return ret.ToArray();
        }
        [HttpGet]
        public string[] SearchImage(string name)
        {
            return mBing.SearchImage(name);
        }
        [HttpGet]
        public async Task<UserStateModel[]> GetUsersInRoom(string room)
        {
            var r = GrainFactory.GetGrain<IChatRoom>(room);
            var users = await r.getUsersInRoom();

            UserStateModel[] u = new UserStateModel[users.Count];

            for (int i = 0; i < users.Count; i++)
                u[i] = new UserStateModel
                {
                    Id = users[i].Id,
                    Name = users[i].Name,
                    Language = users[i].Language,
                    State = users[i].State,
                    Room = users[i].Room,
                    Avatar = users[i].Avatar
                };
            return u;
        }

        //[HttpGet]
        //public async Task<ChatMessageModel[]> GetMessagesInRoom(string room, string userId, string language)
        //{
        //    var r = GrainFactory.GetGrain<IChatRoom>(room);
        //    var messages = await r.getMessagesInRoom(userId);

        //    ChatMessageModel[] m = new ChatMessageModel[messages.Count];

        //    for (int i = 0; i < messages.Count; i++)
        //        m[i] = new ChatMessageModel
        //        {
        //            Sender = messages[i].Sender,
        //            SrcLanguage = messages[i].SenderLanguage,
        //            TgtLanguage = messages[i].ReceiverLanguage,
        //            SrclText = messages[i].Text,
        //            TgtText = messages[i].TranslatedText
        //        };
        //    return m;
        //}

        [HttpGet]
        public async Task<MessageTranslationsModel> DisputeTranslation(string srcLang, string tgtLang, string original, string translated, string tag)
        {
            var t = GrainFactory.GetGrain<ITranslations>(Utils.makeHashKey(srcLang, tgtLang, original));
            var translations = await t.disputeTranslation();

            if (translations != null && translations.Count > 0)
            {
                MessageTranslationModel[] ret = new MessageTranslationModel[translations.Count];
                for (int i = 0; i < ret.Length; i++)
                    ret[i] = new MessageTranslationModel
                    {
                        RowKey = translations[i].RowKey,
                        Text = translations[i].Text,
                        Rank = translations[i].Rank,
                        IsBing = translations[i].IsBing
                    };
                
                return new MessageTranslationsModel
                {
                    PartitionKey = Utils.makeHashKey(srcLang, tgtLang, original),
                    Translations = ret,
                    OriginalText = original,
                    OriginalLanguage = srcLang,
                    TargetLanguage = tgtLang
                };
            }
            else
                return null;
        }

        [HttpGet]
        public async Task<MessageTranslationModel> AddTranslation(string srcLang, string tgtLang, string original, string translated)
        {
            var t = GrainFactory.GetGrain<ITranslations>(Utils.makeHashKey(srcLang, tgtLang, original));
            var translation = await t.addTranslation(srcLang, tgtLang, original, translated, false, 0);

            return new MessageTranslationModel {
                RowKey = translation.RowKey,
                Text = translation.Text,
                Rank = translation.Rank,
                IsBing = translation.IsBing
            };
        }

        [HttpGet]
        public async Task<int> Vote(string partition, string row, int offset)
        {
            var t = GrainFactory.GetGrain<ITranslations>(partition);
            var vote = await t.voteTranslation(row, offset);

            return vote;
        }
    }
}
