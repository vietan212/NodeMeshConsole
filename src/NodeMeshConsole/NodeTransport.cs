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
            this._log = log;
            this.LocalAddress = NetworkAddressProvider.GetActiveIPv4Address();
            this.StreamPort = StreamPortBase + (int)nodeIdentity;
            this.ReliablePort = ReliablePortBase + (int)nodeIdentity;
            this._peers = new ConcurrentDictionary<string, PeerNode>(StringComparer.OrdinalIgnoreCase);

            this._streamTransport = new StreamTransport(nodeIdentity.ToString(), this.StreamPort, this._log);
            this._streamTransport.MessageReceived += (sender, token, payload) =>
                this._log(string.Format("[stream] {0}::{1} <= {2}", sender, token, PayloadCodec.Describe(payload)));

            this._reliableTransport = new ReliableTransport(nodeIdentity.ToString(), this.ReliablePort, this._log);
            this._reliableTransport.MessageReceived += (sender, token, payload) =>
                this._log(string.Format("[reliable] {0}::{1} <= {2}", sender, token, PayloadCodec.Describe(payload)));

            this._beaconTransport = new BeaconTransport(
                nodeIdentity.ToString(),
                this.LocalAddress.ToString(),
                BeaconPort,
                this.StreamPort,
                this.ReliablePort,
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(4),
                this._log);

            this._beaconTransport.PeerUpserted += this.OnPeerUpserted;
            this._beaconTransport.PeerExpired += this.OnPeerExpired;
        }

        public NodeIdentity NodeIdentity { get; private set; }

        public IPAddress LocalAddress { get; private set; }

        public int StreamPort { get; private set; }

        public int ReliablePort { get; private set; }

        public IReadOnlyCollection<PeerNode> Peers
        {
            get { return this._peers.Values.OrderBy(peer => peer.NodeId, StringComparer.OrdinalIgnoreCase).ToArray(); }
        }

        public IReadOnlyCollection<LatestStreamValue> LatestStreamValues
        {
            get { return this._streamTransport.LatestValues; }
        }

        public void Start()
        {
            this._streamTransport.Start();
            this._reliableTransport.Start();
            this._beaconTransport.Start();

            this._log(string.Format(
                "Node {0} online at {1} (beacon {2}, PUB {3}, ROUTER {4})",
                this.NodeIdentity,
                this.LocalAddress,
                BeaconPort,
                this.StreamPort,
                this.ReliablePort));
        }

        public void PublishStream(string token, TransportPayload payload)
        {
            this._streamTransport.Publish(token, payload);
            this._log(string.Format("[stream] local::{0} => {1}", token, PayloadCodec.Describe(payload)));
        }

        public void BroadcastReliable(string token, TransportPayload payload)
        {
            this._reliableTransport.Broadcast(token, payload);
            this._log(string.Format("[reliable] local::{0} => {1}", token, PayloadCodec.Describe(payload)));
        }

        public void Dispose()
        {
            this._beaconTransport.Dispose();
            this._streamTransport.Dispose();
            this._reliableTransport.Dispose();
        }

        private void OnPeerUpserted(PeerNode peer)
        {
            var current = this._peers.AddOrUpdate(peer.NodeId, peer, (_, __) => peer);
            this._streamTransport.ConnectPeer(current);
            this._reliableTransport.ConnectPeer(current);
            this._log(string.Format("[beacon] peer active: {0}", current));
        }

        private void OnPeerExpired(PeerNode peer)
        {
            PeerNode removed;
            if (this._peers.TryRemove(peer.NodeId, out removed))
            {
                this._streamTransport.DisconnectPeer(removed);
                this._reliableTransport.DisconnectPeer(removed);
                this._log(string.Format("[beacon] peer removed: {0}", removed.NodeId));
            }
        }
    }
}
