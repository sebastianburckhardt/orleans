using Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
#pragma warning disable 1998

public class SocketRequest : ISocketRequest
{
    public SocketRequest(int numreqs)
    {
        this.numreqs = numreqs;
    }

    private int numreqs;

    // server/client state
    private int count;

    public string Signature
    {
        get { return "WS hello?numreqs=" + numreqs; }
    }

    public async Task ProcessConnectionOnServer(ISocket socket)
    {
        Util.Assert(count == 0);
    }

    public async Task ProcessMessageOnServer(ISocket socket, string message)
    {
        Util.Assert(message == "Hello #" + count++, "incorrect message from client");
        await socket.Send(message);
    }

    public async Task ProcessCloseOnServer(ISocket socket, string message)
    {
        Util.Assert(count == numreqs);
        Util.Assert(message == "completed");
        await socket.Close("ack");
    }

    public async Task ProcessConnectionOnClient(ISocket socket)
    {
        Util.Assert(count == 0);
        await socket.Send("Hello #" + count);
    }

    public async Task ProcessMessageOnClient(ISocket socket, string message)
    {
        Util.Assert(message == "Hello #" + count);
        if (++count < numreqs)
            await socket.Send("Hello #" + count);
        else
            await socket.Close("completed");
    }

    public async Task ProcessCloseOnClient(ISocket socket, string message)
    {
        Util.Fail("connection closed by server");
    }
}
