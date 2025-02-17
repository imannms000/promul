using Promul.Relay.Protocol;
using Promul.Relay.Server.Relay.Sessions;

namespace Promul.Relay.Server.Relay;

public class RelayServer
{
    private readonly ILoggerFactory _factory;

    private readonly ILogger<RelayServer> _logger;

    private readonly Dictionary<string, RelaySession> _sessionsByCode = new();
    private readonly Dictionary<int, RelaySession> _sessionsByPeer = new();

    public RelayServer(ILogger<RelayServer> logger, ILoggerFactory factory)
    {
        _logger = logger;
        _factory = factory;
        PromulManager = new PromulManager();


        PromulManager.OnReceive += OnNetworkReceive;
        PromulManager.OnConnectionRequest += OnConnectionRequest;
        PromulManager.OnPeerConnected += OnPeerConnected;
        PromulManager.OnPeerDisconnected += OnPeerDisconnected;
    }

    public PromulManager PromulManager { get; }

    public Dictionary<string, RelaySession> GetAllSessions()
    {
        return _sessionsByCode;
    }

    public void CreateSession(string joinCode)
    {
        _sessionsByCode[joinCode] = new RelaySession(joinCode, this, _factory.CreateLogger<RelaySession>());
    }

    public RelaySession? GetSession(string joinCode)
    {
        return _sessionsByCode.GetValueOrDefault(joinCode);
    }

    public async Task DestroySession(RelaySession session)
    {
        //foreach (var peer in session.Peers) _sessionsByPeer.Remove(peer.Id);

        await session.DisconnectAll();
        _sessionsByCode.Remove(session.JoinCode);
    }

    public async ValueTask OnNetworkReceive(PeerBase peer, CompositeReader reader, byte channelNumber,
        DeliveryMethod deliveryMethod)
    {
        var packet = reader.ReadRelayControlMessage();
        string joinCode = System.Text.Encoding.UTF8.GetString(packet.JoinCode);

        //_logger.LogInformation($"OnNetworkReceive # JoinCode: {joinCode}");

        const string format = "Disconnecting {} ({}) because {}";
        if (!_sessionsByCode.TryGetValue(joinCode, out var session))
        //if (!_sessionsByPeer.TryGetValue(peer.Id, out var session))
        {
            _logger.LogInformation(format, peer.Id, peer.EndPoint, "because they are not attached to a session.");
            await PromulManager.DisconnectPeerAsync(peer);
            return;
        }

        await session.OnReceive(peer, packet, deliveryMethod);
    }
    public async ValueTask OnConnectionRequest(ConnectionRequest request)
    {
        var joinCode = request.Data.ReadString();

        if (!_sessionsByCode.TryGetValue(joinCode, out var keyedSession))
        {
            const string format = "Rejecting {} because {}";
            _logger.LogInformation(format, request.RemoteEndPoint,
                "because they requested to join a session that does not exist.");
            await request.RejectAsync(force: true);
            return;
        }

        var peer = await request.AcceptAsync();

        peer.JoinCode = joinCode;

        // Assign ID 0 to the host (server)
        if (keyedSession.HostPeer == null) // First peer is the host
        {
            peer.Id = 0; // Host (server) always has ID 0
        }
        else
        {
            // Assign IDs starting from 1 to clients
            peer.Id = keyedSession.Peers.Count(); // Clients start from 1
        }

        await keyedSession.OnJoinAsync(peer);
        //_sessionsByPeer[peer.Id] = keyedSession;
    }

    public async ValueTask OnPeerConnected(PeerBase peer)
    {
        _logger.LogInformation($"Connected to {peer.EndPoint}");
    }

    public async ValueTask OnPeerDisconnected(PeerBase peer, DisconnectInfo disconnectInfo)
    {
        _logger.LogInformation(
            $"Peer {peer.Id} disconnected: {disconnectInfo.Reason} {disconnectInfo.SocketErrorCode}");
        //if (_sessionsByPeer.TryGetValue(peer.Id, out var session))
        if (_sessionsByCode.TryGetValue(peer.JoinCode, out var session))
        {
            await session.OnLeave(peer);
            //_sessionsByPeer.Remove(peer.Id);
        }
    }
}