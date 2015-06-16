var roomId = '';
var chat;
var isUIReady = false;
var viewModel = new appViewModel();
var typing = false;

//----------View Modes----------
function messageTranslationVieMode(partition, row, text, rank, isBing, parent) {
    this.Text = text;
    this.Rank = ko.observable(rank);
    this.PartitionKey = partition;
    this.RowKey = row;
    this.IsBing = ko.observable(isBing);
    var self = this;
    this.voteUp = function () {
        $.getJSON('/api/chatapi/vote?partition=' + self.PartitionKey + '&row=' + self.RowKey + '&offset=1', function (data) {
            self.Rank(data);
        });
    }
    this.voteDown = function () {
        $.getJSON('/api/chatapi/vote?partition=' + self.PartitionKey + '&row=' + self.RowKey + '&offset=-1', function (data) {
            self.Rank(data);
        });
    }
    this.selectText = function () {
        //TODO: select text
    }
}
function tranlationsViewModel() {
    var self = this;
    this.Translations = ko.observableArray([]);
    this.PartitionKey = '';
    this.OriginalText = '';
    this.OriginalLanguage = '';
    this.TargetLanguage = '';
    this.NewTranslation = ko.observable('');
    this.Initialize = function (entity) {
        if (entity != null) {
            self.Translations.removeAll();
            self.PartitionKey = entity.PartitionKey;
            self.OriginalText = entity.OriginalText;
            self.OriginalLanguage = entity.OriginalLanguage;
            self.TargetLanguage = entity.TargetLanguage;
            self.NewTranslation('');
            $.each(entity.Translations, function (i, val) {
                var model = new messageTranslationVieMode(self.PartitionKey, val.RowKey, val.Text, val.Rank, val.IsBing, self);
                self.Translations.push(model);
                model.Rank.subscribe(function () {
                    self.sortTranslations();
                });
            });
            self.sortTranslations();
        }
    }
    this.addTranslation = function () {
        $.getJSON('/api/chatapi/addtranslation?srcLang=' + self.OriginalLanguage + '&tgtLang=' + self.TargetLanguage + '&original=' + encodeURIComponent(self.OriginalText) + '&translated=' + encodeURIComponent(self.NewTranslation()), function (val) {
            var model = new messageTranslationVieMode(self.PartitionKey, val.RowKey, val.Text, val.Rank, val.IsBing, self);
            self.Translations.push(model);
            model.Rank.subscribe(function () {
                self.sortTranslations();
            });
            self.sortTranslations();
        });
    }
    this.sortTranslations = function () {
        self.Translations.sort(function (left, right) {
            return left.Rank() == right.Rank() ? 0 : (left.Rank() < right.Rank() ? 1 : -1);
        });
    }
}
function appViewModel() {
    var self = this;
    this.AlternativeTranslations = new tranlationsViewModel();
    this.Users = ko.observableArray([]);
    this.Languages = ko.observableArray([]);
    this.ChatLines = ko.observableArray([]);
    this.CurrentUser = new userStateViewModel('', 'user', 'en', '/Images/avatar1.png', '');
    this.CurrentLanguage = ko.observable('en');
    this.RoomParticipants = ko.observable(0);
    this.RoomParticipantsDisplay = ko.computed(function () {
        return "(" + this.RoomParticipants() + ")";
    },this);
    this.hideElement = function (element) {
        if (element.nodeType === 1) $(element).fadeOut('fast', function () { $(element).remove(); });
    };
}
function userStateViewModel(id, name, language, avatar, state) {
    var self = this;
    this.Id = ko.observable(id);
    this.Name = ko.observable(name);
    this.Language = ko.observable(language);
    this.Avatar = ko.observable(avatar);
    this.State = ko.observable(state);
    this.Remove = ko.observable(false);
    this.nameChanged = function (data, event) {
        if (!isUIReady)
            return;
        if (event.keyCode == 13) {
            updateStatus('Updating name ...', false);
            chat.setName(txtChatroom.value, self.Id(), txtName.value);
        }
    };
    this.roomChanged = function (data, event) {
        if (!isUIReady)
            return;
        if (event.keyCode == 13) {
            joinRoom();
        }
    }
}
function chatLineViewModel(user, translated, original, srcLanguage) {
    this.User = user;
    this.Translated = translated;
    this.Original = original;
    this.SrcLanguage = srcLanguage;
    this.dispute = function (data, event) {
        disputeMessage(event.target, this.SrcLanguage, this.Original, this.Translated);
    }
}
function languageViewModel(code, name) {
    this.Code = code;
    this.Name = name;
}

//----------SignalR Callbacks----------
function rejoinRoomRequested(room, id, text) {
    updateStatus('Connection to room ' + room + ' is lost. Re-joining ...', true);
    chat.joinRoom(roomId, room, id, txtName.value, selLanguages.value);
}
function roomJoined(room) {
    roomId = room.Name;
    if (viewModel.CurrentUser.Id() == '' || room.UserId == viewModel.CurrentUser.Id()) {
        viewModel.CurrentUser.Id(room.UserId);
        txtChatroom.value = room.Name;
        txtName.value = room.UserName;
        txtParticipants.innerText = '(' + room.ParticipantCount + ')';
    }
    else if (txtChatroom.value == room.Name) {
        txtParticipants.innerText = '(' + room.ParticipantCount + ')';
    }
    updateStatus('Welcome ' + room.UserName + ' to chat room: ' + room.Name, true);
    setTimeout(refreshRoomUsers, 3000);
}
function nameChanged(user) {
    txtName.value = user.Name;
    updateStatus('Successfully changed your name to ' + user.Name, true);
}
function messageReceived(message) {
    appendChatText(message.Sender, message.TgtText, message.SrclText, message.SrcLanguage);
}
function userStateChanged(user) {
    var match = ko.utils.arrayFirst(viewModel.Users(), function (item) { return item.Id() == user.Id; });
    if (!match) {
        var model = new userStateViewModel(user.Id, user.Name, user.Language, user.Avatar, user.State);
        viewModel.Users.push(model);
    }
    else {
        match.Avatar(user.Avatar);
        match.Name(user.Name);
        match.Language(user.Language);
        match.State(user.State);
        if (user.Remove)
            viewModel.Users.remove(match);
    }
    if (viewModel.CurrentUser.Id() == user.Id) {
        viewModel.CurrentUser.Avatar(user.Avatar);
        viewModel.CurrentUser.Name(user.Name);
        viewModel.CurrentUser.Language(user.Language);
        viewModel.CurrentUser.State(user.State);
    }
    viewModel.RoomParticipants(viewModel.Users().length);
    if (!user.Remove) {
        setTimeout(function () {
            fadeUserState(user.Id);
        }, 2000);
    }
}

function appendChatText(user, message, original, originalLanguage) {
    var match = ko.utils.arrayFirst(viewModel.Users(), function (item) { return item.Id() == user; });
    if (match != null) {
        viewModel.ChatLines.push(new chatLineViewModel(match, message, original, originalLanguage));
        $("#divDisplay").animate({ scrollTop: $("#divDisplay").prop("scrollHeight") }, 300);
    }
}

//----------UI Event Handlers----------
$('#txtChat').keyup(function (e) {
    if (!isUIReady)
        return;
    if (!typing && e.keyCode != 13) {
        typing = true;
        chat.notifyTyping(txtChatroom.value, viewModel.CurrentUser.Id());
        setTimeout(function () {
            typing = false;
        }, 2000);
    }
    if (e.keyCode == 13) {
        if (txtChat.value != '') {
            chat.sendMessage(txtChatroom.value, viewModel.CurrentUser.Id(), txtChat.value);
            txtChat.value = ''; //todo: handle error
            typing = false;
        }
    }
});
$('#divDisplay').click(hideTranslations());
$('#userDisplay').click(hideTranslations());
$('#txtImgSearch').keyup(function (e) {
    if (e.keyCode != 13) {
        var url = "";
        if (txtImgSearch.value.match("^@"))
            url = "http://api.twitter.com/1/users/profile_image?screen_name=" + txtImgSearch.value;
        else
            url = "http://graph.facebook.com/" + txtImgSearch.value.replace(/ /g, ".") + "/picture";
        $('#imgList').empty();
        $("<img src='" + url + "' class='avatar' onclick=\"selectAvatar('" + url + "')\" />").appendTo("#imgList");
        addErrorhandler();
    }
});

//----------Client-side Functions----------
function joinRoom() {
    chat.joinRoom(roomId, txtChatroom.value, viewModel.CurrentUser.Id(), txtName.value, selLanguages.value);
}
function refreshRoomUsers() {
    $.getJSON('/api/chatapi/getusersinroom?room=' + roomId, function (data) {
        viewModel.Users.removeAll();
        $.each(data, function (key, val) {
            userStateChanged(val);
        });
    });
}
function fadeUserState(id) {
    var match = ko.utils.arrayFirst(viewModel.Users(), function (item) { return item.Id() == id; });
    if (match) {
        match.State('');
    };
}
function disputeMessage(elm, srcLanguage, original, translated) {    
    if (srcLanguage != selLanguages.value) {
        $.getJSON('/api/chatapi/disputetranslation?srcLang=' + srcLanguage + '&tgtLang=' + selLanguages.value + '&original=' + encodeURIComponent(original) + '&translated=' + encodeURIComponent(translated) + '&tag=a', function (data) {
            viewModel.AlternativeTranslations.Initialize(data);
            $('#translations').css({ 'left': ($(elm).offset().left) + 'px', 'top': ($(elm).offset().top - 58) + 'px' });
            $('#translations').fadeIn('fast');
            $('.star_div').mouseover(function () {
                $('#img_' + $(this).attr('id')).fadeIn('fast');
            });
            $('.star_div').mouseout(function () {
                $('#img_' + $(this).attr('id')).fadeOut('fast');
            });
        });
    }
}
function hideTranslations() {
    $('#translations').fadeOut('fast');
}
function changeAvatar() {
    txtImgSearch.value = txtName.value;
    $('#imgPopup').css({ 'left': ($('#imgUser').offset().left - 10) + 'px', 'top': ($('#imgUser').offset().top + 20) + 'px' });
    $('#imgPopup').fadeIn('fast');
    searchImage();
}
function addErrorhandler() {
    var rand = 1 + Math.floor(Math.random() * 8);
    $('#imgList img').error(function () {
        $(this).attr("src", "/Images/avatar" + rand + ".png");
    });
}
function searchImage() {
    $.getJSON('/api/chatapi/searchimage?name=' + encodeURIComponent(txtImgSearch.value), function (data) {
        $('#imgList').empty();
        $.each(data, function (key, val) {
            $("<img src='" + val + "' class='avatar' onclick=\"selectAvatar('" + val + "')\" />").appendTo("#imgList");
        });
        addErrorhandler();
    });
}
function selectAvatar(url) {
    chat.setAvatar(txtChatroom.value, viewModel.CurrentUser.Id(), url);
    $('#imgPopup').fadeOut('fast');
}
function updateStatus(text, fade) {
    $('#footer_text').fadeIn().text(text);
    if (fade)
        setInterval("clearStatus('" + text + "')", 3000);
}
function clearStatus(text) {
    if ($('#footer_text').text() == text)
        $('#footer_text').fadeOut();
}

$(document).ready(function () {
    updateStatus('Loading ... OK', false);

    chat = $.connection.chathub;
    chat.roomJoined = roomJoined;
    chat.messageReceived = messageReceived;
    chat.nameChanged = nameChanged;
    chat.userStateChanged = userStateChanged;
    chat.rejoinRoomRequested = rejoinRoomRequested;

    $.getJSON('/api/chatapi/getlanguages', function (data) {
        viewModel.Languages.removeAll();
        $.each(data, function (key, val) {
            viewModel.Languages.push(new languageViewModel(val.Code, val.Name));
        });
        selLanguages.value = 'en';
        $.connection.hub.start(joinRoom);
        updateStatus('Joining room ...', false);


        viewModel.CurrentLanguage.subscribe(function (newValue) {
            chat.setLanguage(txtChatroom.value, viewModel.CurrentUser.Id(), newValue);
        });

        isUIReady = true;
        txtChat.focus();
    });
    ko.bindingHandlers.fadeVisible = {
        init: function (element, valueAccessor) {
            // Initially set the element to be instantly visible/hidden depending on the value
            var value = valueAccessor();
            $(element).toggle(ko.utils.unwrapObservable(value)); // Use "unwrapObservable" so we can handle values that may or may not be observable
        },
        update: function (element, valueAccessor) {
            // Whenever the value subsequently changes, slowly fade the element in or out
            var value = valueAccessor();
            ko.utils.unwrapObservable(value) ? $(element).fadeIn() : $(element).fadeOut();
        }
    };

    viewModel.AlternativeTranslations.Translations.subscribe(function () {
        hideTranslations();
    });
    ko.applyBindings(viewModel);

});
