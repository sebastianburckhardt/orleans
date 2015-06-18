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

// <copyright file="ChatHub.cs" company="open-source" >
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
using System.Threading.Tasks;
using System.Web;
using MultiLingualChat.Entities;
using MultiLingualChat.Web.Models;
using SignalR.Hubs;
using SignalR;

using MultiLingualChat.GrainInterfaces;
using Orleans;
using Orleans.Runtime.Host;
using System.Threading;

namespace MultiLingualChat.Web
{
    [HubName("chathub")]
    public class ChatHub : Hub, IDisconnect, IConnected
    {
        private TemporarySignalRHub tsr = new TemporarySignalRHub();

        #region Client connect status changes

        //TODO: disconnect from SignalR / room
        public Task Disconnect() //Client disconnected
        {
            var user = tsr.getUser(Context.ConnectionId);

            if (user != null)
            {
                var room = GrainFactory.GetGrain<IChatRoom>(user.Room);
                room.leaveRoom(user.Id);
            }
            // TODO: get rid of this temp code
            return tsr.userDisconnected(Context.ConnectionId, user);
        }

        //TODO: connect to room
        public Task Connect() //Client connected
        {
            return tsr.userConnected(Context.ConnectionId);
        }
        //TODO: reconnect to room
        public Task Reconnect(IEnumerable<string> groups)
        {
            return tsr.userReconnected(Context.ConnectionId);
        }
        #endregion

        public async Task JoinRoom(string oldRoom, string room, string userId, string userName, string language)
        {
            System.Diagnostics.Debug.WriteLine("Called join room: " + room + "; " + userId);
            // user belonged to a different room, needs to remove her from old room.

            if (!string.IsNullOrEmpty(oldRoom) && oldRoom != room)
            {
                var oRoom = GrainFactory.GetGrain<IChatRoom>(oldRoom);
                await oRoom.leaveRoom(userId);
            }
            
            var nRoom = GrainFactory.GetGrain<IChatRoom>(room);
            var roomState = await nRoom.joinRoom(userId, userName, language);

            //TODO: get rid of this temp code
            tsr.joinRoom(Context.ConnectionId, oldRoom, room, language, roomState);
        }

        public async Task<ChatUserModel> SetName(string room, string userId, string userName)
        {
            //System.Diagnostics.Debug.WriteLine("Changing name in room: " + room + " for user id: " + id + " to new name:" + user);
            var r = GrainFactory.GetGrain<IChatRoom>(room);
            var userState = await r.setName(userId, userName);

            var state = new ChatUserModel
            {
                Id = userState.Id,
                Language = userState.Language,
                Name = userState.Name,
                Avatar = userState.Avatar,
            };

            // TODO: get rid of temp code
            tsr.setName(Context.ConnectionId, room, state);
            return state;
        }

        public void NotifyTyping(string room, string id)
        {
            //tsr.notifyTyping(string room, string id);
            //var user = mCache.GetUser(id);
            //Clients[room].userStateChanged(UserStateModel.FromChatUserModel(user, room, "is typing"));
        }

        public async Task SetAvatar(string room, string userId, string url)
        {
            var r = GrainFactory.GetGrain<IChatRoom>(room);
            var userState = await r.setAvatar(userId, url);

            //TODO: get rid of this
            if (userState.oldAvatar != userState.Avatar)
            {
                tsr.setAvatar(room, userState);
            }
        }

        public async Task SetLanguage(string room, string userId, string language)
        {
            var r = GrainFactory.GetGrain<IChatRoom>(room);
            var userState = await r.setLanguage(userId, language);

            //TODO: get rid of this
            if (userState.oldLanguage != userState.Language)
            {
                tsr.setLanguage(Context.ConnectionId, room, userState);
            }
        }

        public async Task SendMessage(string room, string userId, string text)
        {
            var r = GrainFactory.GetGrain<IChatRoom>(room);
            var messages = await r.sendMessage(userId, text);

            System.Diagnostics.Debug.WriteLine("Messages = " + messages);
            // TODO: get rid of it
            tsr.sendMessages(messages, room, userId, text);
        }
    }

    public class TemporarySignalRHub
    {
        private Dictionary<string, UserStateModel> _users = new Dictionary<string, UserStateModel>();
        private readonly RoomStatePollingService _pollingService;

        public TemporarySignalRHub()
        {
            _users = new Dictionary<string, UserStateModel>();
            _pollingService = new RoomStatePollingService();
            _pollingService.startPolling();
        }

        private string LanguageRoom = "{0}-{1}";

        public UserStateModel getUser(string connectionId) {
            var hub = GlobalHost.ConnectionManager.GetHubContext<ChatHub>();
            UserStateModel user; 
            if (_users.TryGetValue(connectionId, out user)) {
                return user;
            }
            return null;
        }

        public void addUser(string connectionId, UserStateModel user) {
            _users.Add(connectionId, user);
            _pollingService.addRoom(user.Room);
        }

        public void removeUser(string connectionId, UserStateModel user) {
            _users.Remove(connectionId);
            _pollingService.removeRoom(user.Room);
        }

        public Task userDisconnected(string connectionId, UserStateModel user)
        {
            var hub = GlobalHost.ConnectionManager.GetHubContext<ChatHub>();
            if (user != null)
            {
                var model = new UserStateModel
               {
                   Id = user.Id,
                   Room = user.Room,
                   Language = user.Language,
                   Name = user.Name,
                   State = "left room",
                   Avatar = user.Avatar,
                   Remove = true
               };
               // Notify at least the clients I know about
               hub.Clients[user.Room].userStateChanged(model);
            }
            removeUser(connectionId, user);
            return hub.Clients.leave(connectionId, DateTime.Now.ToString());
        }

        public Task userConnected(string connectionId)
        {
            var hub = GlobalHost.ConnectionManager.GetHubContext<ChatHub>();
            return hub.Clients.joined(connectionId, DateTime.Now.ToString());
        }

        public Task userReconnected(string connectionId)
        {
            var hub = GlobalHost.ConnectionManager.GetHubContext<ChatHub>();
            return hub.Clients.rejoined(connectionId, DateTime.Now.ToString());
        }

        public void joinRoom(string connectionId, string oldRoom, string room, string language, RoomState chatRoom)
        {
            var hub = GlobalHost.ConnectionManager.GetHubContext<ChatHub>();

            //user belonged to a different room, needs to remove her from old room.
            if (oldRoom != room && !string.IsNullOrEmpty(oldRoom))
            {
                hub.Groups.Remove(connectionId, oldRoom).Wait(); //group for chatroom state updates
                hub.Groups.Remove(connectionId, string.Format(LanguageRoom, oldRoom, language)).Wait(); //group for specific language updates   
                hub.Clients[oldRoom].userStateChanged(
                new UserStateModel
                {
                    Id = chatRoom.UserId,
                    Language = language,
                    Name = chatRoom.UserName,
                    Room = oldRoom,
                    State = "left room",
                    Avatar = chatRoom.UserAvatar,
                    Remove = true
                });
            }
            hub.Groups.Add(connectionId, room).Wait(); //group for chatroom state updates
            hub.Groups.Add(connectionId, string.Format(LanguageRoom, room, language)).Wait(); //group for specific language updates
            var state = new UserStateModel
            {
                Id = chatRoom.UserId,
                Language = language,
                Name = chatRoom.UserName,
                Room = room,
                State = "joined room",
                Avatar = chatRoom.UserAvatar,
                Remove = false
            };

            addUser(connectionId, state);

            System.Diagnostics.Debug.WriteLine("Calling roomJoined and userStateChanged with: " + chatRoom.Name);
            
            hub.Clients[room].roomJoined(chatRoom);
            hub.Clients[room].userStateChanged(state);
        }

        public void setName(string connectionId, string room, ChatUserModel user)
        {
            var hub = GlobalHost.ConnectionManager.GetHubContext<ChatHub>();
            hub.Clients[connectionId].nameChanged(user);
            hub.Clients[room].userStateChanged(UserStateModel.FromChatUserModel(user, room, "changed name"));
        }

        public void setAvatar(string room, UserState userState)
        {
            var hub = GlobalHost.ConnectionManager.GetHubContext<ChatHub>();
            var state = new ChatUserModel
            {
                Id = userState.Id,
                Language = userState.Language,
                Name = userState.Name,
                Avatar = userState.Avatar,
            };
            hub.Clients[room].userStateChanged(UserStateModel.FromChatUserModel(state, room, "changed avatar"));
        }

        public void setLanguage(string connectionId, string room, UserState userState)
        {
            var hub = GlobalHost.ConnectionManager.GetHubContext<ChatHub>();
            var state = new ChatUserModel
            {
                Id = userState.Id,
                Language = userState.Language,
                Name = userState.Name,
                Avatar = userState.Avatar,
            };

            hub.Groups.Remove(connectionId, string.Format(LanguageRoom, room, userState.oldLanguage)).Wait();
            hub.Groups.Add(connectionId, string.Format(LanguageRoom, room, userState.Language)).Wait();
            hub.Clients[room].userStateChanged(UserStateModel.FromChatUserModel(state, room, "changed language"));
        }

        public void notifyTyping(string room, string userId) {
            var user = getUser(userId);
            //Clients[room].userStateChanged(UserStateModel.FromChatUserModel(user, room, "is typing"));
        }

        public void sendMessages(List<ChatMessage> messages, string room, string userId, string text)
        {
            var hub = GlobalHost.ConnectionManager.GetHubContext<ChatHub>();
            if (messages != null && messages.Count != 0)
            {
                foreach (var message in messages)
                {
                    var m = new ChatMessageModel
                    {
                        Sender = message.Sender,
                        SrcLanguage = message.SenderLanguage,
                        TgtLanguage = message.ReceiverLanguage,
                        SrclText = message.Text,
                        TgtText = message.TranslatedText
                    };
                    hub.Clients[string.Format(LanguageRoom, room, message.ReceiverLanguage)].messageReceived(m);
                }
            }
            else
            {
                //we've lost state. Re-invite everybody back to the room
                hub.Clients[room].rejoinRoomRequested(room, userId, text);
            }
        } 
    }

    public class RoomStatePollingService
    {
        private Thread _workerThread;
        private AutoResetEvent _finished;
        private const int _timeout =  1 * 1000;
        private Dictionary<string, CompleteRoomState> _rooms; // TODO: is this concurrent?

        public void startPolling()
        {
            _rooms = new Dictionary<string, CompleteRoomState>();
            _workerThread = new Thread(poll);
            _finished = new AutoResetEvent(false);
            _workerThread.Start();
        }

        public void addRoom(string room) {
            if (!_rooms.ContainsKey(room))
            {
                _rooms.Add(room, new CompleteRoomState {connectedUsers = 0});
            }

            var state = _rooms[room];
            state.connectedUsers++;
            _rooms[room] = state;
        }

        public void removeRoom(string room)
        {
            var state = _rooms[room];
            if (state.connectedUsers == 1)
                _rooms.Remove(room);
            else
            {
                state.connectedUsers--;
                _rooms[room] = state;
            }
        }

        private async void poll()
        {
            while (!_finished.WaitOne(_timeout))
            {
                foreach (var item in _rooms)
                {
                    await _sendUserStateUpdates(item.Key, item.Value);
                    await _sendMessageUpdates(item.Key, item.Value);
                }
            }
        }

        private async Task _sendUserStateUpdates(string roomName, CompleteRoomState roomState) { // TODO: check if this is pointer reference
            var hub = GlobalHost.ConnectionManager.GetHubContext<ChatHub>();
            var r = GrainFactory.GetGrain<IChatRoom>(roomName);
            var currentUsers = await r.getUsersInRoom();
            var localUsers = roomState.users;

            //Compare users in the grain with the local state
            if (roomState.users == null)
            {
                roomState.users = currentUsers;
            }
            else
            {
                //TODO: more efficiently
                var usersToRemove = localUsers.Except(currentUsers, new UserComparer()).ToList();
                var usersToAdd = currentUsers.Except(localUsers, new UserComparer()).ToList();
                var usersStateChanged = (currentUsers.Except(usersToAdd, new UserComparer()).ToList()).Except(localUsers, new UsersStrictComparer()).ToList();

                foreach (var item in usersToRemove) {
                    hub.Clients[roomName].userStateChanged(
                        new UserStateModel {
                            Id = item.Id,
                            Language = item.Language,
                            Name = item.Name,
                            Room = roomName,
                            State = "left room",
                            Avatar = item.Avatar,
                            Remove = true
                        });
                }

                foreach (var item in usersToAdd)
                {
                    hub.Clients[roomName].userStateChanged(
                        new UserStateModel
                        {
                            Id = item.Id,
                            Language = item.Language,
                            Name = item.Name,
                            Room = roomName,
                            State = "joined room",
                            Avatar = item.Avatar,
                            Remove = false
                        });
                }

                foreach (var item in usersStateChanged)
                {
                    hub.Clients[roomName].userStateChanged(
                        new UserStateModel
                        {
                            Id = item.Id,
                            Language = item.Language,
                            Name = item.Name,
                            Room = roomName,
                            State = "profile changed",
                            Avatar = item.Avatar,
                            Remove = false
                        });
                }

                roomState.users = currentUsers;
            }
        }

        private async Task _sendMessageUpdates(string roomName, CompleteRoomState roomState)
        {
            var hub = GlobalHost.ConnectionManager.GetHubContext<ChatHub>();
            var r = GrainFactory.GetGrain<IChatRoom>(roomName);
            var currentMessages = await r.getOriginalMessagesInRoom();
            var localMessages = roomState.messages;

            if (localMessages == null)
            {
                roomState.messages = currentMessages;
            }
            else
            {
                var messagesToAdd = currentMessages.Except(localMessages, new MessageComparer()).ToList();
                foreach (var item in messagesToAdd)
                {
                    hub.Clients[roomName].messageReceived(new ChatMessageModel
                    {
                        Sender = item.Sender,
                        SrcLanguage = item.SenderLanguage,
                        TgtLanguage = item.ReceiverLanguage,
                        SrclText = item.Text,
                        TgtText = item.TranslatedText
                    });
                }
            }
        }

        public void stopPolling()
        {
            _finished.Set();
            _workerThread.Join();
        }

        public struct CompleteRoomState
        {
            public int connectedUsers { get; set; }
            public List<UserState> users { get; set; }
            public List<ChatMessage> messages { get; set; }
        }

        public class UsersStrictComparer : IEqualityComparer<UserState>
        {
            public bool Equals(UserState x, UserState y)
            {
                return x.Id == y.Id &&
                       x.Language == y.Language && 
                       x.Name == y.Name && 
                       x.oldAvatar == y.oldAvatar &&
                       x.oldLanguage == y.oldLanguage &&
                       x.Room == y.Room &&
                       x.State == y.State;
            }

            public int GetHashCode(UserState obj)
            {
                return obj.Id.GetHashCode();
            }
        }

        public class UserComparer : IEqualityComparer<UserState>
        {
            public bool Equals(UserState x, UserState y)
            {
                return x.Id == y.Id;
            }

            public int GetHashCode(UserState obj)
            {
                return obj.Id.GetHashCode();
            }
        }

        public class MessageComparer : IEqualityComparer<ChatMessage>
        {
            //TODO: message id received from the client
            public bool Equals(ChatMessage x, ChatMessage y)
            {
                return x.Sender == y.Sender &&
                       x.SenderLanguage == y.SenderLanguage &&
                       x.ReceiverLanguage == y.ReceiverLanguage &&
                       x.Text == y.Text;
            }

            public int GetHashCode(ChatMessage obj)
            {
                return obj.Text.GetHashCode();
            }
        }
    }
}