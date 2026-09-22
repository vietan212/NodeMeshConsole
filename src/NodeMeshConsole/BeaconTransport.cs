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
            _port = port;
            _broadcastInterval = broadcastInterval;
            _peerTimeout = peerTimeout;
            _log = log;
            _peers = new ConcurrentDictionary<string, PeerNode>(StringComparer.OrdinalIgnoreCase);
            _localBeacon = new BeaconTransportMessage
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
            _cancellationTokenSource = new CancellationTokenSource();
            _broadcastClient = new UdpClient(AddressFamily.InterNetwork);
            _broadcastClient.EnableBroadcast = true;

            _receiveClient = new UdpClient(AddressFamily.InterNetwork);
            _receiveClient.EnableBroadcast = true;
            _receiveClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            _receiveClient.Client.Bind(new IPEndPoint(IPAddress.Any, _port));

            _broadcastTask = Task.Run(() => BroadcastLoopAsync(_cancellationTokenSource.Token));
            _receiveTask = Task.Run(() => ReceiveLoopAsync(_cancellationTokenSource.Token));
            _pruneTask = Task.Run(() => PruneLoopAsync(_cancellationTokenSource.Token));
        }

        public void Dispose()
        {
            if (_cancellationTokenSource == null)
            {
                return;
            }

            _cancellationTokenSource.Cancel();

            if (_broadcastClient != null)
            {
                _broadcastClient.Close();
                _broadcastClient.Dispose();
                _broadcastClient = null;
            }

            if (_receiveClient != null)
            {
                _receiveClient.Close();
                _receiveClient.Dispose();
                _receiveClient = null;
            }

            try
            {
                Task.WaitAll(new[] { _broadcastTask, _receiveTask, _pruneTask }.Where(task => task != null).ToArray(), TimeSpan.FromSeconds(2));
            }
            catch (AggregateException)
            {
            }
            catch (ObjectDisposedException)
            {
            }

            _cancellationTokenSource.Dispose();
            _cancellationTokenSource = null;
        }

        private async Task BroadcastLoopAsync(CancellationToken cancellationToken)
        {
            var destination = new IPEndPoint(IPAddress.Broadcast, _port);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    _localBeacon.SentUtcTicks = DateTime.UtcNow.Ticks;
                    var bytes = MessagePackSerializer.Serialize(_localBeacon);
                    await _broadcastClient.SendAsync(bytes, bytes.Length, destination).ConfigureAwait(false);
                    await Task.Delay(_broadcastInterval, cancellationToken).ConfigureAwait(false);
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
                    result = await _receiveClient.ReceiveAsync().ConfigureAwait(false);
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
                if (string.Equals(beacon.NodeId, _localBeacon.NodeId, StringComparison.OrdinalIgnoreCase))
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

                _peers.AddOrUpdate(peer.NodeId, peer, (_, __) => peer);
                this.PeerUpserted?.Invoke(peer);
            }
        }

        private async Task PruneLoopAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(_broadcastInterval, cancellationToken).ConfigureAwait(false);
                    var threshold = DateTimeOffset.UtcNow - _peerTimeout;

                    foreach (var pair in _peers.ToArray())
                    {
                        if (pair.Value.LastSeenUtc >= threshold)
                        {
                            continue;
                        }

                        PeerNode removedPeer;
                        if (_peers.TryRemove(pair.Key, out removedPeer))
                        {
                            _log(string.Format("[beacon] peer timeout for {0}", removedPeer.NodeId));
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
