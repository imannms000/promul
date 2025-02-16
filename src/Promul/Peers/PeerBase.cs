﻿#if DEBUG
#define STATS_ENABLED
#endif
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
namespace Promul
{
    /// <summary>
    ///     Represents a remote peer, managed by a given <see cref="PromulManager" /> instance.
    /// </summary>
    public partial class PeerBase
    {
        private const int MtuCheckDelay = 1000;
        private const int MaxMtuCheckAttempts = 4;
        private const int ShutdownDelay = 300;
        private readonly ChannelBase?[] _channels;

        private readonly ConcurrentQueue<ChannelBase> _channelSendQueue; // Queue for regular messages
        private readonly Dictionary<ushort, ushort> _deliveredFragments;
        private readonly Dictionary<ushort, IncomingFragments> _holdedFragments;

        // Merging
        private NetworkPacket _mergeData;
        private readonly SemaphoreSlim _mtuMutex = new(1, 1);

        private NetworkPacket _pingPacket = NetworkPacket.FromProperty(PacketProperty.Pong, 0);
        private readonly Stopwatch _pingTimer = new();
        private NetworkPacket _pongPacket = NetworkPacket.FromProperty(PacketProperty.Ping, 0);

        private readonly SemaphoreSlim _shutdownSemaphore // Locks for shutdown operations
            = new(1, 1);

        // Channels
        private readonly Queue<NetworkPacket> _unreliableChannel; // Unreliable packet queue

        private readonly SemaphoreSlim _unreliableChannelSemaphore = // Unreliable channel lock
            new(1, 1);

        /// <summary>
        ///     The local ID of this peer.
        /// </summary>
        public readonly int Id;

        /// <summary>
        ///     the Join Code used by this peer.
        /// </summary>
        public string JoinCode; // temporrary solution

        /// <summary>
        ///     The <see cref="PromulManager" /> instance responsible for this peer.
        /// </summary>
        public readonly PromulManager PromulManager;

        /// <summary>
        ///     The network statistics for this connection.
        /// </summary>
        public readonly NetStatistics Statistics = new();

        // Connection
        private int _connectionAttempts;
        private byte _connectNumber;
        private long _connectTimer;

        // MTU negotiation and checking
        private int
            _currentMtuIndex; // The current index in NetConstants.PossibleMtu, representing the current negotiated MTU

        private int _fragmentId;
        private int _mergeCount;
        private int _mergePos;

        private int
            _mtuCheckAttempts; // Number of times an MTU check has been attempted since the last successful negotation

        private long _mtuCheckTimer; // Milliseconds since last MTU CHECK
        private bool _mtuNegotiationComplete; // Whether negotiation is complete
        private long _pingSendTimer; // Milliseconds since last ping sent
        private int _rtt; // Current round-trip time
        private int _rttCount; // Number of times _rtt has been updated
        private long _rttResetTimer; // Milliseconds since last RTT reset
        private NetworkPacket? _shutdownPacket;
        private long _shutdownTimer;
        protected long _timeSinceLastPacket; // Milliseconds since last packet of any type received

        internal volatile PeerBase? NextPeer;
        internal PeerBase? PrevPeer;

        protected PeerBase(PromulManager promulManager, IPEndPoint remote, int id,
            long connectTime, byte connectionNumber)
        {
            Id = id;
            Statistics = new NetStatistics();
            PromulManager = promulManager;
            ResetMtu();
            EndPoint = remote;
            ConnectionState = ConnectionState.Connected;
            _mergeData = NetworkPacket.FromProperty(PacketProperty.Merged, NetConstants.MaxPacketSize);
            _pongPacket = NetworkPacket.FromProperty(PacketProperty.Pong, 0);
            _pingPacket = NetworkPacket.FromProperty(PacketProperty.Ping, 0);
            _pingPacket.Sequence = 1;

            _unreliableChannel = new Queue<NetworkPacket>();
            _holdedFragments = new Dictionary<ushort, IncomingFragments>();
            _deliveredFragments = new Dictionary<ushort, ushort>();

            _channels = new ChannelBase[promulManager.ChannelsCount * NetConstants.ChannelTypeCount];
            _channelSendQueue = new ConcurrentQueue<ChannelBase>();

            ConnectTime = connectTime;
            ConnectionNumber = connectionNumber;
        }

        internal byte ConnectionNumber
        {
            get => _connectNumber;
            set
            {
                _connectNumber = value;
                _mergeData.ConnectionNumber = value;
                _pingPacket.ConnectionNumber = value;
                _pongPacket.ConnectionNumber = value;
            }
        }

        /// <summary>
        ///     The remote endpoint of this peer.
        /// </summary>
        public IPEndPoint EndPoint { get; private set; }

        /// <summary>
        ///     The current connection state of this peer.
        /// </summary>
        public ConnectionState ConnectionState { get; protected set; }

        /// <summary>
        ///     Connection time for internal purposes
        /// </summary>
        internal long ConnectTime { get; }

        /// <summary>
        ///     Our ID, according to the remote peer.
        /// </summary>
        public int RemoteId { get; protected set; }

        /// <summary>
        ///     The current ping to this remote peer, in milliseconds.
        ///     This value is calculated by halving <see cref="RoundTripTime" />.
        /// </summary>
        public int Ping => RoundTripTime / 2;

        /// <summary>
        ///     The current time to complete a round-trip request to this remote peer, in milliseconds.
        /// </summary>
        public int RoundTripTime { get; private set; }

        /// <summary>
        ///     The current maximum transfer unit, that is, the maximum size of a given UDP packet
        ///     that will not cause fragmentation.
        /// </summary>
        public int MaximumTransferUnit { get; private set; }

        /// <summary>
        ///     The current delta between the remote peer's time and the <see cref="PromulManager" />'s local time.
        ///     A positive value indicates the remote peer is ahead of local time.
        /// </summary>
        public long RemoteTimeDelta { get; private set; }

        /// <summary>
        ///     The time, in UTC, of the remote peer.
        /// </summary>
        public DateTime RemoteUtcTime => new(DateTime.UtcNow.Ticks + RemoteTimeDelta);

        /// <summary>
        ///     The time, in milliseconds, since the last packet was received from this peer.
        /// </summary>
        public long TimeSinceLastPacket => _timeSinceLastPacket;

        internal double ResendDelay { get; private set; } = 27.0;

        internal void InitiateEndPointChange()
        {
            ResetMtu();
            ConnectionState = ConnectionState.EndPointChange;
        }

        internal void FinishEndPointChange(IPEndPoint newEndPoint)
        {
            if (ConnectionState != ConnectionState.EndPointChange)
                return;
            ConnectionState = ConnectionState.Connected;
            EndPoint = newEndPoint;
        }

        internal void ResetMtu()
        {
            _mtuNegotiationComplete = false;
            if (PromulManager.MtuOverride > 0)
                OverrideMtu(PromulManager.MtuOverride);
            else if (PromulManager.UseSafeMtu)
                SetMtu(0);
            else
                SetMtu(1);
        }

        private void SetMtu(int mtuIdx)
        {
            _currentMtuIndex = mtuIdx;
            MaximumTransferUnit = NetConstants.PossibleMtu[mtuIdx] - PromulManager.ExtraPacketSizeForLayer;
        }

        private void OverrideMtu(int mtuValue)
        {
            MaximumTransferUnit = mtuValue;
            _mtuNegotiationComplete = true;
        }

        /// <summary>
        ///     Returns the number of packets in queue for sending in the given reliable channel.
        /// </summary>
        /// <param name="channelNumber">The number of the channel to query.</param>
        /// <param name="ordered">
        ///     If true, this method will query the reliable-ordered channel, otherwise, the reliable-unordered
        ///     channel.
        /// </param>
        /// <returns>The number of packets remaining in the given queue.</returns>
        public int GetRemainingReliableQueuePacketCount(byte channelNumber, bool ordered)
        {
            var idx = channelNumber * NetConstants.ChannelTypeCount +
                      (byte)(ordered ? DeliveryMethod.ReliableOrdered : DeliveryMethod.ReliableUnordered);
            var channel = _channels[idx];
            return (channel as ReliableChannel)?.PacketsInQueue ?? 0;
        }

        private ChannelBase CreateChannel(byte idx)
        {
            var newChannel = _channels[idx];
            if (newChannel != null)
                return newChannel;

            newChannel = (DeliveryMethod)(idx % NetConstants.ChannelTypeCount) switch
            {
                DeliveryMethod.ReliableUnordered => new ReliableChannel(this, false, idx),
                DeliveryMethod.Sequenced => new SequencedChannel(this, false, idx),
                DeliveryMethod.ReliableOrdered => new ReliableChannel(this, true, idx),
                DeliveryMethod.ReliableSequenced => new SequencedChannel(this, true, idx),
                _ => throw new InvalidOperationException(
                    $"CreateChannel requested for delivery method {(DeliveryMethod)(idx % NetConstants.ChannelTypeCount):G}, which is not channeled!")
            };
            var prevChannel = Interlocked.CompareExchange(ref _channels[idx], newChannel, null);
            return prevChannel ?? newChannel;
        }

        internal static async Task<OutgoingPeer> ConnectToAsync(PromulManager manager, IPEndPoint remote, int id,
            byte connectionNumber, ArraySegment<byte> data)
        {
            var peer = new OutgoingPeer(manager, remote, id, connectionNumber, data);

            await peer.SendConnectionRequestAsync();
            return peer;
        }

        internal static async Task<IncomingPeer> AcceptAsync(PromulManager promulManager, ConnectionRequest request,
            int id)
        {
            var peer = new IncomingPeer(promulManager, request.RemoteEndPoint, id,
                request.InternalPacket.PeerId,
                request.InternalPacket.ConnectionTime, request.InternalPacket.ConnectionNumber);

            await peer.SendAcceptedConnectionAsync();

            return peer;
        }


        /// <summary>
        ///     Gets the maximum size of user-provided data that can be sent without fragmentation.
        ///     This method subtracts the size of the relevant packet headers.
        /// </summary>
        /// <param name="options">The type of packet to be calculated.</param>
        /// <returns>The maximum transmission unit size, in bytes, for the queried packet type.</returns>
        public int GetUserMaximumTransmissionUnit(DeliveryMethod options)
        {
            return MaximumTransferUnit - NetworkPacket.GetHeaderSize(options == DeliveryMethod.Unreliable
                ? PacketProperty.Unreliable
                : PacketProperty.Channeled);
        }

        /// <summary>
        ///     Sends a data stream to the remote peer. This method will queue the data in the correct
        ///     delivery channel, so completion of this method does NOT indicate completion of the
        ///     sending process.
        /// </summary>
        /// <param name="data">The data to transmit.</param>
        /// <param name="channelNumber">The number of channel to send on.</param>
        /// <param name="deliveryMethod">The delivery method to send the data.</param>
        /// <exception cref="TooBigPacketException">
        ///     Thrown in the following instances:<br />
        ///     - The size of <see cref="data" /> exceeds <see cref="GetUserMaximumTransmissionUnit" /> if
        ///     <see cref="DeliveryMethod" /> is <see cref="DeliveryMethod.Unreliable" />.<br />
        ///     - The number of computed fragments exceeds <see cref="ushort.MaxValue" />.
        /// </exception>
        public Task SendAsync(ArraySegment<byte> data, DeliveryMethod deliveryMethod = DeliveryMethod.ReliableOrdered,
            byte channelNumber = 0)
        {
            return SendInternal(data, channelNumber, deliveryMethod);
        }

        private async Task SendInternal(
            ArraySegment<byte> data,
            byte channelNumber,
            DeliveryMethod deliveryMethod)
        {
            var length = data.Count;
            if (ConnectionState != ConnectionState.Connected || channelNumber >= _channels.Length)
                return;

            //Select channel
            PacketProperty property;
            ChannelBase? channel = null;

            if (deliveryMethod == DeliveryMethod.Unreliable)
            {
                property = PacketProperty.Unreliable;
            }
            else
            {
                property = PacketProperty.Channeled;
                channel = CreateChannel((byte)(channelNumber * NetConstants.ChannelTypeCount + (byte)deliveryMethod));
            }

            //Check fragmentation
            var headerSize = NetworkPacket.GetHeaderSize(property);
            //Save mtu for multithread
            var mtu = MaximumTransferUnit;
            var completePackageSize = headerSize + data.Count;
            if (data.Count + headerSize > mtu)
            {
                //if cannot be fragmented
                if (deliveryMethod != DeliveryMethod.ReliableOrdered &&
                    deliveryMethod != DeliveryMethod.ReliableUnordered)
                    throw new TooBigPacketException(
                        $"Packets larger than {mtu - headerSize} (MTU) bytes are only permitted to be sent via reliable and non-sequenced delivery methods.");

                var maxMtuCarryingCapacity = mtu - headerSize;
                var packetDataSize = maxMtuCarryingCapacity - NetConstants.FragmentHeaderSize;
                var totalPackets = data.Count / packetDataSize + (data.Count % packetDataSize == 0 ? 0 : 1);

 //                NetDebug.Write($@"Preparing to send {data.Count} bytes of fragmented data.
 // Complete data size (header + data): {completePackageSize}
 // Current MTU: {mtu}
 // Size of header for {property:G}: {headerSize}
 // Size of fragmentation header: {NetConstants.FragmentHeaderSize}
 // Maximum possible data per packet (MTU-header-fragment header): {packetDataSize}
 // That means we must send {totalPackets} total packets.");

                if (totalPackets > ushort.MaxValue)
                    throw new TooBigPacketException("Data was split in " + totalPackets + " fragments, which exceeds " +
                                                    ushort.MaxValue);

                var currentFragmentId = (ushort)Interlocked.Increment(ref _fragmentId);

                for (ushort partIdx = 0; partIdx < totalPackets; partIdx++)
                {
                    var sendLength = data.Count > packetDataSize ? packetDataSize : data.Count;


                    if (data.Array != null)
                    {
                        var srcOffset = data.Offset + partIdx * packetDataSize;
                        var srcCount = sendLength;
                        if (srcOffset + srcCount > data.Count) srcCount = data.Count - srcOffset;
                        var p = NetworkPacket.FromProperty(property, srcCount + NetConstants.FragmentHeaderSize);
                        p.FragmentId = currentFragmentId;
                        p.FragmentPart = partIdx;
                        p.FragmentsTotal = (ushort)totalPackets;
                        p.MarkFragmented();
                        Buffer.BlockCopy(data.Array,
                            srcOffset, p.Data.Array, p.Data.Offset + NetConstants.FragmentedHeaderTotalSize, srcCount);
                        if (channel != null) await channel.EnqueuePacketAsync(p);
                    }

                    length -= sendLength;
                }

                return;
            }

            //Else just send
            var packet = NetworkPacket.FromProperty(property, length);
            if (data.Array != null)
                Buffer.BlockCopy(data.Array, data.Offset,
                    packet.Data.Array, packet.Data.Offset + headerSize, length);

            if (channel == null) //unreliable
            {
                _unreliableChannelSemaphore.Wait();
                _unreliableChannel.Enqueue(packet);
                _unreliableChannelSemaphore.Release();
            }
            else
            {
                await channel.EnqueuePacketAsync(packet);
            }
        }

        internal DisconnectResult ProcessDisconnect(NetworkPacket packet)
        {
            if ((ConnectionState == ConnectionState.Connected || ConnectionState == ConnectionState.Outgoing) &&
                packet.Data.Count >= 9 &&
                BitConverter.ToInt64(packet.Data[1..]) == ConnectTime &&
                packet.ConnectionNumber == _connectNumber)
                return ConnectionState == ConnectionState.Connected
                    ? DisconnectResult.Disconnect
                    : DisconnectResult.Reject;
            return DisconnectResult.None;
        }

        internal void AddToReliableChannelSendQueue(ChannelBase channel)
        {
            _channelSendQueue.Enqueue(channel);
        }

        internal async Task<ShutdownResult> ShutdownAsync(ArraySegment<byte> data, bool force)
        {
            await _shutdownSemaphore.WaitAsync();
            try
            {
                if (ConnectionState is ConnectionState.Disconnected or ConnectionState.ShutdownRequested)
                    return ShutdownResult.None;

                var result = ConnectionState == ConnectionState.Connected
                    ? ShutdownResult.WasConnected
                    : ShutdownResult.Success;

                if (force)
                {
                    ConnectionState = ConnectionState.Disconnected;
                    return result;
                }

                Interlocked.Exchange(ref _timeSinceLastPacket, 0);

                var sp = NetworkPacket.FromProperty(PacketProperty.Disconnect, data.Count);
                sp.ConnectionNumber = _connectNumber;
                _shutdownPacket = sp;
                FastBitConverter.GetBytes(_shutdownPacket.Value.Data.Array, _shutdownPacket.Value.Data.Offset + 1, ConnectTime);
                if (_shutdownPacket.Value.Data.Count >= MaximumTransferUnit)
                    //Drop additional data
                    LogDebug("Disconnect additional data size more than MTU - 8!");
                else if (data != null && data.Count > 0)
                    data.CopyTo(_shutdownPacket.Value.Data.Array, _shutdownPacket.Value.Data.Offset + 9);
                ConnectionState = ConnectionState.ShutdownRequested;
                await PromulManager.RawSendAsync(_shutdownPacket.Value, EndPoint);
                return result;
            }
            finally
            {
                _shutdownSemaphore.Release();
            }
        }

        private void UpdateRoundTripTime(int roundTripTime)
        {
            _rtt += roundTripTime;
            _rttCount++;
            RoundTripTime = _rtt / _rttCount;
            ResendDelay = 25.0 + RoundTripTime * 2.1; // 25 ms + double rtt
        }

        internal async ValueTask AddReliablePacket(DeliveryMethod method, NetworkPacket p)
        {
            if (p.IsFragmented)
            {
                var packetFragId = p.FragmentId;
                var packetChannelId = p.ChannelId;

                if (!_holdedFragments.TryGetValue(packetFragId, out var incomingFragments))
                {
                    incomingFragments = new IncomingFragments
                    {
                        Fragments = new NetworkPacket?[p.FragmentsTotal],
                        ChannelId = p.ChannelId
                    };
                    _holdedFragments.Add(packetFragId, incomingFragments);
                }

                //Cache
                var fragments = incomingFragments.Fragments;

                //Error check 
                if (p.FragmentPart >= fragments.Length ||
                    fragments[p.FragmentPart] != null ||
                    p.ChannelId != incomingFragments.ChannelId)
                {
                    LogDebug(
                        $"Fragmented packet {p.FragmentId} (channel {incomingFragments.ChannelId}): received invalid fragment part {p.FragmentId + 1} (channel {p.ChannelId})");
                    return;
                }

                //Fill array
                fragments[p.FragmentPart] = p;

                //Increase received fragments count
                Interlocked.Increment(ref incomingFragments.ReceivedCount);

                //Increase total size
                incomingFragments.TotalSize += p.Data.Count - NetConstants.FragmentedHeaderTotalSize;

                _holdedFragments[packetFragId] = incomingFragments;

                //Check for finish
                if (incomingFragments.ReceivedCount != fragments.Length)
                    return;

                //just simple packet
                var resultingPacket = NetworkPacket.FromBuffer(new byte[incomingFragments.TotalSize]);

                var pos = 0;
                for (var i = 0; i < incomingFragments.ReceivedCount; i++)
                {
                    var fragment = fragments[i];
                    if (fragment.HasValue)
                    {
                        var writtenSize = fragment.Value.Data.Count - NetConstants.FragmentedHeaderTotalSize;

                        if (pos + writtenSize > resultingPacket.Data.Count)
                        {
                            _holdedFragments.Remove(packetFragId);
                            LogDebug(
                                $"Fragment error pos: {pos + writtenSize} >= resultPacketSize: {resultingPacket.Data.Count} , totalSize: {incomingFragments.TotalSize}");
                            return;
                        }

                        if (fragment.Value.Data.Count > fragment.Value.Data.Count)
                        {
                            _holdedFragments.Remove(packetFragId);
                            LogDebug(
                                $"Fragment error size: {fragment.Value.Data.Count} > fragment.RawData.Length: {fragment.Value.Data.Count}");
                            return;
                        }

                        //Create resulting big packet
                        Buffer.BlockCopy(
                            fragment.Value.Data.Array,
                            fragment.Value.Data.Offset + NetConstants.FragmentedHeaderTotalSize,
                            resultingPacket.Data.Array,
                            resultingPacket.Data.Offset + pos,
                            writtenSize);
                        pos += writtenSize;

                        fragments[i] = null;   
                    }
                }

                //Clear memory
                _holdedFragments.Remove(packetFragId);

                //Send to process
                LogDebug(
                    $"Reconstructed fragmented packet from {fragments.Length} fragments (size {resultingPacket.Data.Count})");
                await PromulManager.CreateReceiveEvent(resultingPacket, method,
                    (byte)(packetChannelId / NetConstants.ChannelTypeCount), 0, this);
            }
            else //Just simple packet
            {
                await PromulManager.CreateReceiveEvent(p, method, (byte)(p.ChannelId / NetConstants.ChannelTypeCount),
                    NetConstants.ChanneledHeaderSize, this);
            }
        }

        //Process incoming packet
        internal async ValueTask ProcessPacket(NetworkPacket packet)
        {
            if (ConnectionState == ConnectionState.Outgoing || ConnectionState == ConnectionState.Disconnected) return;
            if (packet.Property == PacketProperty.ShutdownOk)
            {
                if (ConnectionState == ConnectionState.ShutdownRequested)
                    ConnectionState = ConnectionState.Disconnected;
                return;
            }

            if (packet.ConnectionNumber != _connectNumber)
            {
                LogDebug(
                    $"Received a packet with invalid connection number ({packet.ConnectionNumber}), expected {_connectNumber}. Ignoring.");
                return;
            }

            Interlocked.Exchange(ref _timeSinceLastPacket, 0);

            switch (packet.Property)
            {
                case PacketProperty.Merged:
                    var pos = NetConstants.HeaderSize;
                    while (pos < packet.Data.Count)
                    {
                        var size = BitConverter.ToUInt16(packet.Data.Array, packet.Data.Offset + pos);
                        pos += 2;
                        if (packet.Data.Count - pos < size)
                            break;

                        var mergedPacket = NetworkPacket.FromProperty(PacketProperty.Unknown, size);
                        Buffer.BlockCopy(packet.Data.Array, packet.Data.Offset + pos, mergedPacket.Data.Array,
                            mergedPacket.Data.Offset, size);

                        if (!mergedPacket.Verify())
                            break;

                        pos += size;
                        await ProcessPacket(mergedPacket);
                    }

                    //NetManager.PoolRecycle(packet);
                    break;
                case PacketProperty.Ping:
                    if (NetUtils.RelativeSequenceNumber(packet.Sequence, _pongPacket.Sequence) > 0)
                    {
                        FastBitConverter.GetBytes(_pongPacket.Data.Array, _pongPacket.Data.Offset + 3,
                            DateTime.UtcNow.Ticks);
                        _pongPacket.Sequence = packet.Sequence;
                        await PromulManager.RawSendAsync(_pongPacket, EndPoint);
                    }

                    break;
                case PacketProperty.Pong:
                    if (packet.Sequence == _pingPacket.Sequence)
                    {
                        _pingTimer.Stop();
                        var elapsedMs = (int)_pingTimer.ElapsedMilliseconds;
                        RemoteTimeDelta = BitConverter.ToInt64(packet.Data[3..]) +
                            elapsedMs * TimeSpan.TicksPerMillisecond / 2 - DateTime.UtcNow.Ticks;
                        UpdateRoundTripTime(elapsedMs);
                        await PromulManager.ConnectionLatencyUpdated(this, elapsedMs / 2);
                    }

                    break;
                case PacketProperty.Ack:
                case PacketProperty.Channeled:
                    if (packet.ChannelId >= _channels.Length) break;

                    var channel = _channels[packet.ChannelId] ??
                                  (packet.Property == PacketProperty.Ack ? null : CreateChannel(packet.ChannelId));
                    if (channel != null)
                        if (!await channel.HandlePacketAsync(packet))
                        {
                        }

                    break;

                //Simple packet without acks
                case PacketProperty.Unreliable:
                    await PromulManager.CreateReceiveEvent(packet, DeliveryMethod.Unreliable, 0,
                        NetConstants.HeaderSize, this);
                    return;

                case PacketProperty.MtuCheck:
                case PacketProperty.MtuOk:
                    await ProcessMtuPacketAsync(packet);
                    break;

                default:
                    LogDebug("Error! Unexpected packet type: " + packet.Property);
                    break;
            }
        }


        internal async Task SendUserData(NetworkPacket packet)
        {
            packet.ConnectionNumber = _connectNumber;
            var mergedPacketSize = NetConstants.HeaderSize + packet.Data.Count + 2;
            const int splitThreshold = 20;
            //if (mergedPacketSize + splitThreshold >= MaximumTransferUnit)
            //{
            await PromulManager.RawSendAsync(packet, EndPoint);
            //LogDebug($"[Send] {packet.Property} ({packet.Data.Count} bytes) (sequence {packet.Sequence})");
            //}
            // if (_mergePos + mergedPacketSize > MaximumTransferUnit) await SendMerged();
            //
            // FastBitConverter.GetBytes(_mergeData.Data.Array, _mergeData.Data.Offset+_mergePos + NetConstants.HeaderSize, (ushort)packet.Data.Count);
            // packet.Data.CopyTo(_mergeData.Data.Array, _mergeData.Data.Offset+_mergePos+NetConstants.HeaderSize+2);
            // _mergePos += packet.Data.Count + 2;
            // _mergeCount++;
        }

        private async Task SendMerged()
        {
            if (_mergeCount == 0)
                return;
            int bytesSent;
            if (_mergeCount > 1)
            {
                LogDebug("[P]Send merged: " + _mergePos + ", count: " + _mergeCount);
                bytesSent = await PromulManager.RawSendAsync(
                    new ArraySegment<byte>(_mergeData.Data.Array, _mergeData.Data.Offset,
                        NetConstants.HeaderSize + _mergePos),
                    EndPoint);
            }
            else
            {
                //Send without length information and merging
                bytesSent = await PromulManager.RawSendAsync(
                    new ArraySegment<byte>(_mergeData.Data.Array, _mergeData.Data.Offset + NetConstants.HeaderSize + 2,
                        _mergePos - 2),
                    EndPoint);
            }

            if (PromulManager.RecordNetworkStatistics)
            {
                Statistics.IncrementPacketsSent();
                Statistics.AddBytesSent(bytesSent);
            }

            _mergePos = 0;
            _mergeCount = 0;
        }

        internal async Task Update(long deltaTime)
        {
            Interlocked.Add(ref _timeSinceLastPacket, deltaTime);
            switch (ConnectionState)
            {
                case ConnectionState.Connected:
                    if (_timeSinceLastPacket > PromulManager.DisconnectTimeout)
                    {
                        LogDebug(
                            $"[UPDATE] Disconnect by timeout: {_timeSinceLastPacket} > {PromulManager.DisconnectTimeout}");
                        await PromulManager.ForceDisconnectPeerAsync(this, DisconnectReason.Timeout);
                        return;
                    }

                    break;

                case ConnectionState.ShutdownRequested:
                    if (_timeSinceLastPacket > PromulManager.DisconnectTimeout)
                    {
                        ConnectionState = ConnectionState.Disconnected;
                    }
                    else
                    {
                        _shutdownTimer += deltaTime;
                        if (_shutdownTimer >= ShutdownDelay)
                        {
                            _shutdownTimer = 0;
                            await PromulManager.RawSendAsync(_shutdownPacket.Value, EndPoint);
                        }
                    }

                    return;

                case ConnectionState.Outgoing when this is OutgoingPeer op:
                    _connectTimer += deltaTime;
                    if (_connectTimer > PromulManager.ReconnectDelay)
                    {
                        _connectTimer = 0;
                        _connectionAttempts++;
                        if (_connectionAttempts > PromulManager.MaximumConnectionAttempts)
                        {
                            await PromulManager.ForceDisconnectPeerAsync(this, DisconnectReason.ConnectionFailed);
                            return;
                        }

                        //else send connect again
                        await op.SendConnectionRequestAsync();
                    }

                    return;

                case ConnectionState.Disconnected:
                    return;
            }

            // Send a ping, if we are over the ping interval.
            _pingSendTimer += deltaTime;
            if (_pingSendTimer >= PromulManager.PingInterval)
            {
                _pingSendTimer = 0;
                _pingPacket.Sequence++;
                if (_pingTimer.IsRunning) UpdateRoundTripTime((int)_pingTimer.ElapsedMilliseconds);
                _pingTimer.Restart();
                await PromulManager.RawSendAsync(_pingPacket, EndPoint);
            }

            // Calculate round-trip time, and reset if our RTT values are out of date.
            _rttResetTimer += deltaTime;
            if (_rttResetTimer >= PromulManager.PingInterval * 3)
            {
                _rttResetTimer = 0;
                _rtt = RoundTripTime;
                _rttCount = 1;
            }

            await CheckMtuAsync(deltaTime);

            //Pending send
            var count = _channelSendQueue.Count;
            while (count-- > 0)
            {
                if (!_channelSendQueue.TryDequeue(out var channel))
                    break;
                if (await channel.UpdateQueueAsync())
                    // still has something to send, re-add it to the send queue
                    _channelSendQueue.Enqueue(channel);
            }

            await _unreliableChannelSemaphore.WaitAsync();
            try
            {
                var unreliableCount = _unreliableChannel.Count;
                for (var i = 0; i < unreliableCount; i++)
                {
                    var packet = _unreliableChannel.Dequeue();
                    await SendUserData(packet);
                }
            }
            finally
            {
                _unreliableChannelSemaphore.Release();
            }

            await SendMerged();
        }

        //For reliable channel
        internal async Task RecycleAndDeliver(NetworkPacket packet)
        {
            if (packet.IsFragmented)
            {
                _deliveredFragments.TryGetValue(packet.FragmentId, out var fragCount);
                fragCount++;
                if (fragCount == packet.FragmentsTotal)
                {
                    // TODO FIX THIS
                    await PromulManager.MessageDelivered(this, null);
                    _deliveredFragments.Remove(packet.FragmentId);
                }
                else
                {
                    _deliveredFragments[packet.FragmentId] = fragCount;
                }
            }
            else
            {
                // TODO FIX THIS
                await PromulManager.MessageDelivered(this, null);
            }
        }

        internal abstract Task<ConnectRequestResult> ProcessReconnectionRequestAsync(
            NetConnectRequestPacket connRequest);

        public void LogDebug(string text)
        {
            NetDebug.Write($"[Peer {Id}] {text}");
        }

        // Fragments
        private struct IncomingFragments
        {
            public NetworkPacket?[] Fragments;
            public int ReceivedCount;
            public int TotalSize;
            public byte ChannelId;
        }
    }
}