using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace NodeMeshConsole
{
    internal sealed class NodeTransport : IDisposable
    {
        private const int BeaconPort = 55100;
        private const int StreamPortBase = 56100;
        private const int ReliablePortBase = 57100;
        private readonly ConcurrentDictionary<string, PeerNode> _peers;
        private readonly Action<string> _log;
        private readonly BeaconTransport _beaconTransport;
        private readonly StreamTransport _streamTransport;
        private readonly ReliableTransport _reliableTransport;

        public NodeTransport(NodeIdentity nodeIdentity, Action<string> log)
        {
            this.NodeIdentity = nodeIdentity;
            _log = log;
            this.LocalAddress = NetworkAddressProvider.GetActiveIPv4Address();
            this.StreamPort = StreamPortBase + (int)nodeIdentity;
            this.ReliablePort = ReliablePortBase + (int)nodeIdentity;
            _peers = new ConcurrentDictionary<string, PeerNode>(StringComparer.OrdinalIgnoreCase);

            _streamTransport = new StreamTransport(nodeIdentity.ToString(), this.StreamPort, _log);
            _streamTransport.MessageReceived += (sender, token, payload) =>
                _log(string.Format("[stream] {0}::{1} <= {2}", sender, token, PayloadCodec.Describe(payload)));

            _reliableTransport = new ReliableTransport(nodeIdentity.ToString(), this.ReliablePort, _log);
            _reliableTransport.MessageReceived += (sender, token, payload) =>
                _log(string.Format("[reliable] {0}::{1} <= {2}", sender, token, PayloadCodec.Describe(payload)));

            _beaconTransport = new BeaconTransport(
                nodeIdentity.ToString(),
                this.LocalAddress.ToString(),
                BeaconPort,
                this.StreamPort,
                this.ReliablePort,
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(4),
                _log);

            _beaconTransport.PeerUpserted += OnPeerUpserted;
            _beaconTransport.PeerExpired += OnPeerExpired;
        }

        public NodeIdentity NodeIdentity { get; private set; }

        public IPAddress LocalAddress { get; private set; }

        public int StreamPort { get; private set; }

        public int ReliablePort { get; private set; }

        public IReadOnlyCollection<PeerNode> Peers
        {
            get { return _peers.Values.OrderBy(peer => peer.NodeId, StringComparer.OrdinalIgnoreCase).ToArray(); }
        }

        public IReadOnlyCollection<LatestStreamValue> LatestStreamValues
        {
            get { return _streamTransport.LatestValues; }
        }

        public void Start()
        {
            _streamTransport.Start();
            _reliableTransport.Start();
            _beaconTransport.Start();

            _log(string.Format(
                "Node {0} online at {1} (beacon {2}, PUB {3}, ROUTER {4})",
                this.NodeIdentity,
                this.LocalAddress,
                BeaconPort,
                this.StreamPort,
                this.ReliablePort));
        }

        public void PublishStream(string token, TransportPayload payload)
        {
            _streamTransport.Publish(token, payload);
            _log(string.Format("[stream] local::{0} => {1}", token, PayloadCodec.Describe(payload)));
        }

        public void BroadcastReliable(string token, TransportPayload payload)
        {
            _reliableTransport.Broadcast(token, payload);
            _log(string.Format("[reliable] local::{0} => {1}", token, PayloadCodec.Describe(payload)));
        }

        public void Dispose()
        {
            _beaconTransport.Dispose();
            _streamTransport.Dispose();
            _reliableTransport.Dispose();
        }

        private void OnPeerUpserted(PeerNode peer)
        {
            PeerNode previousPeer;
            _peers.TryGetValue(peer.NodeId, out previousPeer);
            var current = _peers.AddOrUpdate(peer.NodeId, peer, (_, __) => peer);
            var endpointChanged = previousPeer == null ||
                !string.Equals(previousPeer.IpAddress, current.IpAddress, StringComparison.OrdinalIgnoreCase) ||
                previousPeer.StreamPort != current.StreamPort ||
                previousPeer.ReliablePort != current.ReliablePort;

            if (previousPeer != null && endpointChanged)
            {
                _streamTransport.DisconnectPeer(previousPeer);
                _reliableTransport.DisconnectPeer(previousPeer);
            }

            if (endpointChanged)
            {
                _streamTransport.ConnectPeer(current);
                _reliableTransport.ConnectPeer(current);
            }

            _log(string.Format("[beacon] peer active: {0}", current));
        }

        private void OnPeerExpired(PeerNode peer)
        {
            PeerNode removed;
            if (_peers.TryRemove(peer.NodeId, out removed))
            {
                _streamTransport.DisconnectPeer(removed);
                _reliableTransport.DisconnectPeer(removed);
                _log(string.Format("[beacon] peer removed: {0}", removed.NodeId));
            }
        }
    }
}
