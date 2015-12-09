<%@ Page Title="Orleans in Azure" Language="C#" MasterPageFile="~/Site.master" AutoEventWireup="true"
    CodeBehind="Default.aspx.cs" Async="true" Inherits="Orleans.Azure.Samples.Web._Default" %>



<asp:Content ID="HeaderContent" runat="server" ContentPlaceHolderID="HeadContent">
    <title>Orleans in Azure</title>
</asp:Content>

<asp:Content ID="BodyContent" runat="server" ContentPlaceHolderID="MainContent">
    <h2>
        Welcome to Orleans running in Azure!
    </h2>
    <p>&nbsp;</p>
    <asp:ScriptManager ID="ScriptManager1" runat="server"  />
 
    <asp:UpdatePanel ID="UpdatePanel1" runat="server">
        <ContentTemplate>
            <asp:Button ID="ButtonSayHello" runat="server" Text="Ask Local Environment Grain" 
                    onclick="ButtonSayHello_Click" Width="434px" Font-Size="Large" />
            <asp:UpdateProgress ID="UpdateProgress1" runat="server" AssociatedUpdatePanelID="UpdatePanel1">
                <ProgressTemplate>
                    <div id="overlay1">
                        <asp:Label ID="ProgressStatusLabel1" runat="server" Font-Italic="True" Font-Names="Arial" Font-Size="Medium" Text="Waiting for the deployment-local instance of the Hello Environment grain to reply"></asp:Label>
                        <br />
                    </div>
                </ProgressTemplate>
            </asp:UpdateProgress>            
        </ContentTemplate>
    </asp:UpdatePanel>
    <script type="text/javascript"> </script>
    <p></p>
    <asp:UpdatePanel ID="UpdatePanel2" runat="server">
        <ContentTemplate>
                <asp:Button ID="ButtonSayHelloSingleInstance" runat="server" onclick="ButtonSayHelloSingleInstance_Click" Text="Ask Global-Single-Instance Environment Grain" Width="434px" Font-Size="Large" />
                <asp:UpdateProgress ID="UpdateProgress2" runat="server" AssociatedUpdatePanelID="UpdatePanel2">
                    <ProgressTemplate>
                        <div id="overlay2">
                            <asp:Label ID="ProgressStatusLabel2" runat="server" clientIDMode="Static" Font-Italic="True" Font-Names="Arial" Font-Size="Medium" style="margin-left: 2px">Waiting for the global single-instance of the Hello Environment grain to reply</asp:Label>
                        </div>
                    </ProgressTemplate>
                </asp:UpdateProgress>
                <p></p>
                <asp:TextBox ID="ReplyText" runat="server" Font-Names="Courier New" Font-Size="Large" Height="157px" ReadOnly="False" TextMode="MultiLine" Width="100%">Reply messages from Orleans will appear here.</asp:TextBox>
        </ContentTemplate>
    </asp:UpdatePanel>
        
</asp:Content>
