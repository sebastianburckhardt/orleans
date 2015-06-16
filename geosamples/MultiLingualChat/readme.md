<a name="anchor-name-here" />
# Multi-lingual Chat Sample #
The Multi-lingual chat sample demonstrates how to design an interactive, scalable web application. The application allows users to chat with each other in their native languages, while the application automatically translates messages among participants. In addition, the application allows user to provide and vote on alternative translations if they don’t like translations made by Bing Translator Service. 
    
![Chat](https://raw.github.com/WindowsAzure-Samples/Multi-Lingual-Chat/master/images/Chat.png)
<a name="anchor-name-here" />

## Prerequisites ##
- [Visual Studio 2012](http://www.microsoft.com/visualstudio/en-us/products)
- [Azure SDK for .NET 1.7] (http://www.windowsazure.com/en-us/develop/net/)
- An Azure subscription (get a 90-day trail [here](http://www.windowsazure.com/en-us/pricing/free-trial/))
<a name="anchor-name-here" />  
- A Bing Application Id (apply [here](http://www.bing.com/developers/)) 

## Technologies ##
 - ASP.Net MVC 4 Web API  
   _REST-ful service layer for client-side Ajax calls_
 - Bing Search API  
   _Search for images to be used as avatars_
 - Bing Translator API  
   _Translate chat messages_
 - jQuery  
   _Ajax calls to Web API service layer
 - Knockout  
   _View models and data binding to support MVVM pattern_
 - SignalR  
   _Real-time messaging and notificaitons_ 
 - Windows Azure Service Bus  
   _Scalable backplane for SignalR_
 - Windows Azure Table Storage  
   _Save translations and alternative translations with rankings_

<a name="anchor-name-here" />
## Running the Sample Locally ##

1. Open Visual Studio 2012 as an Administrator.
2. Open **MultilingualChat.sln**.
3. Press **F5**!

By default the application is configured to use local simulated storage, and translation capability is turned off without a valid Bing Application ID. SignalR scaling is also disabled when Service Bus connection string is missing. However, you can still try out other chatting functionalities. The application is designed to provide a launch-and-play experience to end users. There’s no registration or login needed, you are automatically put in to a “public” chat room with an automatically assigned user name (and a random avatar) so that you can start chatting right away. You can change chat room, name, avatar, preferred language at any time by clicking on the items you want to change, and then enter or select a new value.

<a name="anchor-name-here" />
## Running the Sample on Windows Azure ##

1. Open Visual Studio 2012 as an Administrator.
2. Open **MultilingualChat.sln**.
3. Open web.config file in **MultiLingualChat.Web** project.
4. Modify **BingAppId** to use your Bing Application Id.
5. Modify **StorageConnectionString** to use your Windows Azure storage account.  
6. (Optional) To enable Service Bus Backplan for SignalR, modify **ServiceBusNamespace**, **ServiceBusAccount**, **ServiceBusAccountKey**, **TopicPathPrefix**, and **NumberOfTopics** to use your Windows Azure Service Bus namespace. Read [this article](https://github.com/SignalR/SignalR/wiki/Azure-service-bus) for more details on these settings.
7. Save changes.
8. Double-click on **MutliLingualChat.Web** role under **MultiLingualChat.Cloud** project to bring up its property page.
9. Select **Caching** tab.
10. In "**Specify the storage account credentials to use for maintaining the cache cluster's runtime state.**" field, enter connection string to your Windows Azure storage account. 
Now you should be able to run the applicaiton locally by pressing **F5**, or to run it on Azure by deploying the Cloud Service.
