using Orleans;
using System;
using System.Linq;
using ReplicatedGrains;
using Orleans.Providers;
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

    [StorageProvider(ProviderName = "AzureStore")]
    public class ChatRoom : SequencedGrain<ChatRoom.State>, IChatRoom
    {
        private BingCache mBing = new BingCache();

        #region State and State Modifiers
        [Serializable]
        public new class State
        {
            public Dictionary<string, UserState> users { get; set; }
            public  List<ChatMessage> messages { get; set; }

            public State()
            {
                users = new Dictionary<string, UserState>();
                messages = new List<ChatMessage>();
            } 
        }

        [Serializable]
        public class StateModifier : IAppliesTo<State>
        {
            private int MAX_MESSAGES = 50;
            public string type { get; set; }
            public string userId { get; set; }
            public UserState chatUser { get; set; }
            public ChatMessage chatMessage { get; set; }
            public void Update(State state)
            {
                switch (type)
                {
                    case "addUser":
                        state.users.Add(userId, chatUser);
                        break;
                    case "removeUser":
                        state.users.Remove(userId);
                        break;
                    case "setName":
                        state.users[userId] = chatUser; //TODO: what if user does not exist?
                        break;
                    case "addMessage":
                        state.messages.Add(chatMessage);
                        if (state.messages.Count > MAX_MESSAGES) {
                            state.messages.RemoveAt(0);
                        }
                        break;
                    default:
                        break;
                }
            }
        }
        #endregion

        public override Task OnActivateAsync() {
            //this.GetPrimaryKeyString();
            return base.OnActivateAsync();
        }

        #region JoinRoom
        public async Task<RoomState> joinRoom(string roomName, string userId, string userName, string language)
        {
            RoomState roomState;
            UserState chatUser;
            var users = (await GetLocalStateAsync()).users;

            if (string.IsNullOrEmpty(userId)) //this is a new user
            {
                chatUser = new UserState
                {
                    Id = Guid.NewGuid().ToString("N"), // TODO: make this globally unique?
                    Name = _getName(userName, new List<UserState>(users.Values)),
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
                        Name = _getName(userName, new List<UserState>(users.Values)),
                        Language = language,
                        Avatar = _generateAvatarUrl()
                    };
                }
            }
            await UpdateLocallyAsync(new StateModifier() { type = "addUser", userId = chatUser.Id, chatUser = chatUser }, false);

            roomState = new RoomState
            {
                Id = Guid.NewGuid().ToString("N"), // TODO: make this globally unique
                Name = roomName,
                ParticipantCount = users.Count + 1, //TODO: makes sense?
                UserId = chatUser.Id,
                UserName = chatUser.Name,
                UserAvatar = chatUser.Avatar
            };
            
            return roomState;
        }

        private string _generateNewName(string name, List<string> existingNames)
        {
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

        private List<string> _getTakenNames(List<UserState> users) 
        {
            List<string> existingNames = new List<string>();

            foreach(var item in users)
            {
                existingNames.Add(item.Name); // TODO: Explore anomaly: adding two users with the same name
            }
            return existingNames;
        }

        private string _getName(string userName, List<UserState> users)
        {
            string name = userName;
            var existingNames = _getTakenNames(users);

            if (existingNames.Contains(userName))
            {
                name = _generateNewName(userName, existingNames);
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
        public async Task leaveRoom(string userId)
        {
            await UpdateLocallyAsync(new StateModifier() { type = "removeUser", userId = userId }, false);
        }
        #endregion

        #region GetUsersInRoom
        public async Task<List<UserState>> getUsersInRoom()
        {
            return new List<UserState>((await GetLocalStateAsync()).users.Values);
        }
        #endregion

        public async Task<UserState> setName(string userId, string userName)
        {
            var users = (await GetLocalStateAsync()).users;

            string name = _getName(userName, new List<UserState>(users.Values));
            UserState chatUser;

            if (users.TryGetValue(userId, out chatUser)) {
                chatUser.Name = name;
                await UpdateLocallyAsync(new StateModifier() { type = "setName", userId = userId, chatUser = chatUser }, false);
                return chatUser;
            }
            else
                // We don't have info about this user
                return new UserState();
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
            var users = (await GetLocalStateAsync()).users;

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

            await UpdateLocallyAsync(new StateModifier() { type = "addMessage", chatMessage = originalMessage }, false);
            
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
