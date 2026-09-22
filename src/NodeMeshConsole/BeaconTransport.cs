using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using MessagePack;

namespace NodeMeshConsole
{
    internal sealed class BeaconTransport : IDisposable
    {
        private readonly BeaconTransportMessage _localBeacon;
        private readonly int _port;
        private readonly TimeSpan _broadcastInterval;
        private readonly TimeSpan _peerTimeout;
        private readonly Action<string> _log;
        private readonly ConcurrentDictionary<string, PeerNode> _peers;
        private CancellationTokenSource _cancellationTokenSource;
        private Task _broadcastTask;
        private Task _receiveTask;
        private Task _pruneTask;
        private UdpClient _broadcastClient;
        private UdpClient _receiveClient;

        public BeaconTransport(
            string nodeId,
            string ipAddress,
            int port,
            int streamPort,
            int reliablePort,
            TimeSpan broadcastInterval,
            TimeSpan peerTimeout,
            Action<string> log)
        {
            this._port = port;
            this._broadcastInterval = broadcastInterval;
            this._peerTimeout = peerTimeout;
            this._log = log;
            this._peers = new ConcurrentDictionary<string, PeerNode>(StringComparer.OrdinalIgnoreCase);
            this._localBeacon = new BeaconTransportMessage
            {
                NodeId = nodeId,
                IpAddress = ipAddress,
                StreamPort = streamPort,
                ReliablePort = reliablePort
            };
        }

        public event Action<PeerNode> PeerUpserted;

        public event Action<PeerNode> PeerExpired;

        public void Start()
        {
            this._cancellationTokenSource = new CancellationTokenSource();
            this._broadcastClient = new UdpClient(AddressFamily.InterNetwork);
            this._broadcastClient.EnableBroadcast = true;

            this._receiveClient = new UdpClient(AddressFamily.InterNetwork);
            this._receiveClient.EnableBroadcast = true;
            this._receiveClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            this._receiveClient.Client.Bind(new IPEndPoint(IPAddress.Any, this._port));

            this._broadcastTask = Task.Run(() => this.BroadcastLoopAsync(this._cancellationTokenSource.Token));
            this._receiveTask = Task.Run(() => this.ReceiveLoopAsync(this._cancellationTokenSource.Token));
            this._pruneTask = Task.Run(() => this.PruneLoopAsync(this._cancellationTokenSource.Token));
        }

        public void Dispose()
        {
            if (this._cancellationTokenSource == null)
            {
                return;
            }

            this._cancellationTokenSource.Cancel();

            if (this._broadcastClient != null)
            {
                this._broadcastClient.Close();
                this._broadcastClient.Dispose();
                this._broadcastClient = null;
            }

            if (this._receiveClient != null)
            {
                this._receiveClient.Close();
                this._receiveClient.Dispose();
                this._receiveClient = null;
            }

            try
            {
                Task.WaitAll(new[] { this._broadcastTask, this._receiveTask, this._pruneTask }.Where(task => task != null).ToArray(), TimeSpan.FromSeconds(2));
            }
            catch (AggregateException)
            {
            }
            catch (ObjectDisposedException)
            {
            }

            this._cancellationTokenSource.Dispose();
            this._cancellationTokenSource = null;
        }

        private async Task BroadcastLoopAsync(CancellationToken cancellationToken)
        {
            var destination = new IPEndPoint(IPAddress.Broadcast, this._port);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    this._localBeacon.SentUtcTicks = DateTime.UtcNow.Ticks;
                    var bytes = MessagePackSerializer.Serialize(this._localBeacon);
                    await this._broadcastClient.SendAsync(bytes, bytes.Length, destination).ConfigureAwait(false);
                    await Task.Delay(this._broadcastInterval, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                UdpReceiveResult result;
                try
                {
                    result = await this._receiveClient.ReceiveAsync().ConfigureAwait(false);
                }
                catch (ObjectDisposedException)
                {
                    return;
                }
                catch (SocketException)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }

                    throw;
                }

                var beacon = MessagePackSerializer.Deserialize<BeaconTransportMessage>(result.Buffer);
                if (string.Equals(beacon.NodeId, this._localBeacon.NodeId, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var peerAddress = SelectPeerAddress(beacon.IpAddress, result.RemoteEndPoint.Address);
                var peer = new PeerNode
                {
                    NodeId = beacon.NodeId,
                    IpAddress = peerAddress.ToString(),
                    StreamPort = beacon.StreamPort,
                    ReliablePort = beacon.ReliablePort,
                    LastSeenUtc = DateTimeOffset.UtcNow
                };

                this._peers.AddOrUpdate(peer.NodeId, peer, (_, __) => peer);
                this.PeerUpserted?.Invoke(peer);
            }
        }

        private async Task PruneLoopAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(this._broadcastInterval, cancellationToken).ConfigureAwait(false);
                    var threshold = DateTimeOffset.UtcNow - this._peerTimeout;

                    foreach (var pair in this._peers.ToArray())
                    {
                        if (pair.Value.LastSeenUtc >= threshold)
                        {
                            continue;
                        }

                        PeerNode removedPeer;
                        if (this._peers.TryRemove(pair.Key, out removedPeer))
                        {
                            this._log(string.Format("[beacon] peer timeout for {0}", removedPeer.NodeId));
                            this.PeerExpired?.Invoke(removedPeer);
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private static IPAddress SelectPeerAddress(string advertisedAddress, IPAddress remoteAddress)
        {
            IPAddress parsedAdvertisedAddress;
            if (!string.IsNullOrWhiteSpace(advertisedAddress) &&
                IPAddress.TryParse(advertisedAddress, out parsedAdvertisedAddress) &&
                parsedAdvertisedAddress.AddressFamily == AddressFamily.InterNetwork &&
                !IPAddress.IsLoopback(parsedAdvertisedAddress) &&
                !IPAddress.Any.Equals(parsedAdvertisedAddress))
            {
                return parsedAdvertisedAddress;
            }

            return remoteAddress;
        }
    }
}
