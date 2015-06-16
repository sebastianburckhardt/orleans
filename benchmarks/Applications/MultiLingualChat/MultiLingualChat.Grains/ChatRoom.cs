using Orleans;
using System;
using System.Linq;
using MultiLingualChat.GrainInterfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using MultiLingualChat.Entities;
using MultiLingualChat.Library;

namespace MultiLingualChat.Grains
{
    /// <summary>
    /// Grain implementation class IChatRoom.
    /// </summary>
    public class ChatRoom : Grain, IChatRoom
    {
        private Dictionary<string, UserState> users;
        private List<ChatMessage> messages;
        private int MAX_MESSAGES = 50;
        private BingCache mBing = new BingCache();

        public override Task OnActivateAsync() {
            users  = new Dictionary<string, UserState>();
            messages = new List<ChatMessage>();
            //this.GetPrimaryKeyString();
            return base.OnActivateAsync();
        }

        #region JoinRoom
        public Task<RoomState> joinRoom(string roomName, string userId, string userName, string language)
        {
            RoomState roomState;
            UserState chatUser;

            if (string.IsNullOrEmpty(userId)) //this is a new user
            {
                chatUser = new UserState
                {
                    Id = Guid.NewGuid().ToString("N"), // TODO: make this globally unique?
                    Name = _getName(userName),
                    Language = language,
                    Avatar = _generateAvatarUrl()
                };
            }
            else
            {
                if (!users.TryGetValue(userId, out chatUser))
                { // not already in room
                    chatUser = new UserState
                    {
                        Id = userId,
                        Name = _getName(userName),
                        Language = language,
                        Avatar = _generateAvatarUrl()
                    };
                }
            }
            users.Add(chatUser.Id, chatUser);

            roomState = new RoomState
            {
                Id = Guid.NewGuid().ToString("N"), // TODO: make this globally unique
                Name = roomName,
                ParticipantCount = users.Count,
                UserId = chatUser.Id,
                UserName = chatUser.Name,
                UserAvatar = chatUser.Avatar
            };
            
            return Task.FromResult(roomState);
        }

        private string _generateNewName(string name)
        {
            var existingNames = _getTakenNames();

            string prefix = name;
            int i = 1;
            while (existingNames.Contains(prefix + i.ToString()) && i <= 1000)
                    i++;
            return prefix + i.ToString();
        }

        //private string _generateNewName(string name)
        //{
        //    var existingNames = _getTakenNames();

        //    Regex regx = new Regex(@"[\d]+$");
        //    Match match = regx.Match(name);
        //    if (match != null && match.Index > 0)
        //    {
        //        string prefix = name.Substring(0, match.Index);
        //        int i = 1;
        //        while (existingNames.Contains(prefix + i.ToString()) && i <= 1000)
        //            i++;
        //        return prefix + i.ToString();
        //    }
        //    else
        //        return name + "1";
        //}

        private List<string> _getTakenNames() 
        {
            List<string> existingNames = new List<string>();
            foreach(var item in users)
            {
                existingNames.Add(item.Value.Name);
            }
            return existingNames;
        }

        private string _getName(string userName)
        {
            string name = userName;
            var existingNames = _getTakenNames();
            if (existingNames.Contains(userName))
            {
                name = _generateNewName(userName);
            }
            return name;
        }

        private string _generateAvatarUrl()
        {
            Random mRand = new Random();
            return "/Images/avatar" + mRand.Next(1, 10) + ".png";
        }

        #endregion

        #region LeaveRoom
        public Task leaveRoom(string userId)
        {
            users.Remove(userId);
            return TaskDone.Done;
        }
        #endregion

        #region GetUsersInRoom
        public Task<List<UserState>> getUsersInRoom()
        {
            return Task.FromResult(new List<UserState>(users.Values));
        }
        #endregion

        public Task<UserState> setName(string userId, string userName)
        {
            string name = _getName(userName);
            UserState chatUser;
            if (users.TryGetValue(userId, out chatUser)) {
                chatUser.Name = name;
                return Task.FromResult(chatUser);
            }
            else
                // We don't have info about this user
                return Task.FromResult(new UserState());
        }

        public Task<UserState> setAvatar(string userId, string url) {
            return Task.FromResult(new UserState());
        }

        public Task<UserState> setLanguage(string userId, string language) {
            return Task.FromResult(new UserState());
        }

        public async Task<List<ChatMessage>> sendMessage(string userId, string text) {
            var messages = await _makeMessages(userId, text);
            System.Diagnostics.Debug.WriteLine("XXX ChatRoom : awaited messages = " + messages);
            return messages;
        }

        private async Task<List<ChatMessage>> _makeMessages(string userId, string text)
        {
            UserState user;
            if (!users.TryGetValue(userId, out user))
            {
                return null; // We have no info about this user (we might have lost state)
            }
            var originalMessage = new ChatMessage
            { 
                Sender = user.Id,
                SenderLanguage = user.Language,
                Text = text
            };

            messages.Add(originalMessage);
            if (messages.Count > MAX_MESSAGES) {
                messages.RemoveAt(0);
            }

            List<ChatMessage> translatedMessages = new List<ChatMessage>();

            foreach (var u in users)
            {
                var message = translatedMessages.FirstOrDefault(m => m.ReceiverLanguage == u.Value.Language);
                // If there's no struct in list satisfying the condition, should return a struct with all fields to null
                if (message.Text == null)
                {
                    if (originalMessage.Sender != u.Value.Id && originalMessage.SenderLanguage != u.Value.Language)
                    {
                        var translations = GrainFactory.GetGrain<ITranslations>(Utils.makeHashKey(user.Language, u.Value.Language, text));
                        var ts = await translations.getAlternativeTranslations();


                        //MessageTranslationEntity entity = new MessageTranslationEntity(originalMessage.SenderLanguage, receiver.Language, text, "");
                        //var array = mRepo.GetAlternativeTranslations(entity);
                        if (ts != null && ts.Count > 0)
                        {
                            originalMessage.TranslatedText = ts[0].Text;
                        }
                        else
                        {
                            originalMessage.TranslatedText = mBing.Translate(originalMessage.Text, originalMessage.SenderLanguage, u.Value.Language);
                            await translations.addTranslation(originalMessage.SenderLanguage, u.Value.Language, text, originalMessage.TranslatedText, true, 10);
                        }
                    }
                    else
                        originalMessage.TranslatedText = originalMessage.Text;

                    translatedMessages.Add(new ChatMessage
                    {
                        Sender = originalMessage.Sender,
                        SenderLanguage = originalMessage.SenderLanguage,
                        ReceiverLanguage = u.Value.Language,
                        Text = originalMessage.Text,
                        TranslatedText = originalMessage.TranslatedText
                    });
                }
            }
            return translatedMessages;
        }
    }
}
