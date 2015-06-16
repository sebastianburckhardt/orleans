using System.Threading.Tasks;
using Orleans;
using System.Collections.Generic;
using System;

namespace MultiLingualChat.GrainInterfaces
{
    /// <summary>
    /// Grain interface IChatRoom
    /// </summary>

    public interface IChatRoom : IGrainWithStringKey
    {
        Task<RoomState> joinRoom(string roomName, string userId, string userName, string language);
        Task leaveRoom(string userId);
        Task<List<UserState>> getUsersInRoom();
        Task<UserState> setName(string userId, string userName);
        Task<UserState> setAvatar(string userId, string url);
        Task<UserState> setLanguage(string userId, string language);
        Task<List<ChatMessage>> sendMessage(string userId, string text);
    }

    [Serializable]
    public struct UserState {
        public string Id {get;set;}
        public string Name { get; set; }
        public string Language { get; set; }
        public string oldLanguage { get; set; }
        public string State { get; set; }
        public string Room { get; set; }
        public string Avatar { get; set; }
        public string oldAvatar { get; set; }
    }

    [Serializable]
    public struct ChatMessage
    {
        public string Sender { get; set; }
        public string SenderLanguage { get; set; }
        public string ReceiverLanguage { get; set; }
        public string Text { get; set; }
        public string TranslatedText { get; set; }
    }

    [Serializable]
    public struct RoomState
    {
        public string Id { get; set; }
        public string UserId { get; set; }
        public string UserName { get; set; }
        public string UserAvatar { get; set; }
        public string Name { get; set; }
        public int ParticipantCount { get; set; }
    }
}
