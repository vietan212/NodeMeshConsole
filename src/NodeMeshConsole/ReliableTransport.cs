using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using NetMQ;
using NetMQ.Sockets;

namespace NodeMeshConsole
{
    internal sealed class ReliableTransport : IDisposable
    {
        private readonly string _localNodeId;
        private readonly int _bindPort;
        private readonly Action<string> _log;
        private readonly NetMQQueue<ReliableCommand> _commandQueue;
        private readonly ManualResetEventSlim _started;
        private readonly ConcurrentDictionary<string, byte> _seenMessages;
        private Thread _workerThread;
        private NetMQPoller _poller;

        public ReliableTransport(string localNodeId, int bindPort, Action<string> log)
        {
            this._localNodeId = localNodeId;
            this._bindPort = bindPort;
            this._log = log;
            this._commandQueue = new NetMQQueue<ReliableCommand>();
            this._started = new ManualResetEventSlim(false);
            this._seenMessages = new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase);
        }

        public event Action<string, string, TransportPayload> MessageReceived;

        public void Start()
        {
            this._workerThread = new Thread(this.RunWorker)
            {
                IsBackground = true,
                Name = "ReliableTransport"
            };
            this._workerThread.Start();
            this._started.Wait(TimeSpan.FromSeconds(5));
        }

        public void Broadcast(string token, TransportPayload payload)
        {
            this._commandQueue.Enqueue(ReliableCommand.CreateBroadcast(token, payload));
        }

        public void ConnectPeer(PeerNode peer)
        {
            this._commandQueue.Enqueue(ReliableCommand.CreateConnect(peer));
        }

        public void DisconnectPeer(PeerNode peer)
        {
            this._commandQueue.Enqueue(ReliableCommand.CreateDisconnect(peer));
        }

        public void Dispose()
        {
            this._commandQueue.Enqueue(ReliableCommand.Stop());

            if (this._workerThread != null && this._workerThread.IsAlive)
            {
                this._workerThread.Join(TimeSpan.FromSeconds(2));
            }

            this._commandQueue.Dispose();
            this._started.Dispose();
        }

        private void RunWorker()
        {
            var dealers = new Dictionary<string, DealerSocket>(StringComparer.OrdinalIgnoreCase);
            var pending = new Dictionary<string, PendingReliableMessage>(StringComparer.OrdinalIgnoreCase);
            var seenOrder = new Queue<string>();

            using (var router = new RouterSocket())
            {
                var retryTimer = new NetMQTimer(TimeSpan.FromMilliseconds(400));
                router.Options.RouterMandatory = false;
                router.Bind("tcp://*:" + this._bindPort);

                this._commandQueue.ReceiveReady += (sender, args) =>
                {
                    ReliableCommand command;
                    while (args.Queue.TryDequeue(out command, TimeSpan.Zero))
                    {
                        if (command.Kind == ReliableCommandKind.Stop)
                        {
                            foreach (var dealer in dealers.Values)
                            {
                                dealer.Dispose();
                            }

                            dealers.Clear();
                            this._poller.Stop();
                            return;
                        }

                        if (command.Kind == ReliableCommandKind.Broadcast)
                        {
                            if (dealers.Count == 0)
                            {
                                this._log("[reliable] no peers available for broadcast.");
                                continue;
                            }

                            var envelope = EnvelopeCodec.CreateData(this._localNodeId, command.Token, command.Payload);
                            var bytes = EnvelopeCodec.Encode(envelope);
                            foreach (var dealerPair in dealers)
                            {
                                SendReliableMessage(dealerPair.Key, dealerPair.Value, envelope, bytes, pending);
                            }

                            continue;
                        }

                        if (command.Peer == null)
                        {
                            continue;
                        }

                        if (command.Kind == ReliableCommandKind.Connect)
                        {
                            if (dealers.ContainsKey(command.Peer.NodeId))
                            {
                                continue;
                            }

                            var dealer = new DealerSocket();
                            dealer.Options.Identity = Encoding.UTF8.GetBytes(this._localNodeId + ":" + command.Peer.NodeId + ":" + Guid.NewGuid().ToString("N"));
                            var peerNodeId = command.Peer.NodeId;
                            var endpoint = "tcp://" + command.Peer.IpAddress + ":" + command.Peer.ReliablePort;
                            dealer.Connect(endpoint);
                            dealer.ReceiveReady += (dealerSender, dealerArgs) =>
                            {
                                while (dealerArgs.Socket.TryReceiveFrameBytes(TimeSpan.Zero, out var frame))
                                {
                                    var ack = EnvelopeCodec.Decode(frame);
                                    if (ack.Kind != EnvelopeKind.Ack || string.IsNullOrWhiteSpace(ack.AcknowledgedMessageId))
                                    {
                                        continue;
                                    }

                                    if (!string.Equals(ack.SenderNode, peerNodeId, StringComparison.OrdinalIgnoreCase))
                                    {
                                        continue;
                                    }

                                    pending.Remove(BuildPendingKey(peerNodeId, ack.AcknowledgedMessageId));
                                    this._log(string.Format("[reliable] ACK {0} from {1}", ack.AcknowledgedMessageId, peerNodeId));
                                }
                            };

                            dealers[peerNodeId] = dealer;
                            this._poller.Add(dealer);
                            this._log(string.Format("[reliable] connected DEALER to {0}", endpoint));
                        }
                        else if (command.Kind == ReliableCommandKind.Disconnect)
                        {
                            DealerSocket dealer;
                            if (!dealers.TryGetValue(command.Peer.NodeId, out dealer))
                            {
                                continue;
                            }

                            dealers.Remove(command.Peer.NodeId);
                            this._poller.Remove(dealer);
                            dealer.Dispose();

                            foreach (var key in pending.Keys.Where(key => key.StartsWith(command.Peer.NodeId + "::", StringComparison.OrdinalIgnoreCase)).ToList())
                            {
                                pending.Remove(key);
                            }

                            this._log(string.Format("[reliable] disconnected DEALER from {0}", command.Peer.NodeId));
                        }
                    }
                };

                router.ReceiveReady += (sender, args) =>
                {
                    var message = new NetMQMessage();
                    while (args.Socket.TryReceiveMultipartMessage(ref message, 0))
                    {
                        if (message.FrameCount < 2)
                        {
                            message = new NetMQMessage();
                            continue;
                        }

                        var dealerIdentity = message[0].ConvertToString(Encoding.UTF8);
                        var envelope = EnvelopeCodec.Decode(message[message.FrameCount - 1].ToByteArray());

                        if (envelope.Kind == EnvelopeKind.Ack)
                        {
                            pending.Remove(BuildPendingKey(dealerIdentity, envelope.AcknowledgedMessageId));
                            message = new NetMQMessage();
                            continue;
                        }

                        var reply = new NetMQMessage();
                        for (var index = 0; index < message.FrameCount - 1; index++)
                        {
                            reply.Append(message[index].ToByteArray());
                        }

                        reply.Append(EnvelopeCodec.Encode(EnvelopeCodec.CreateAck(this._localNodeId, envelope.MessageId)));
                        args.Socket.SendMultipartMessage(reply);

                        if (!this._seenMessages.TryAdd(envelope.MessageId, 0))
                        {
                            message = new NetMQMessage();
                            continue;
                        }

                        seenOrder.Enqueue(envelope.MessageId);
                        if (seenOrder.Count > 4096)
                        {
                            byte ignored;
                            this._seenMessages.TryRemove(seenOrder.Dequeue(), out ignored);
                        }

                        var payload = PayloadCodec.Decode(envelope.PayloadBytes);
                        this.MessageReceived?.Invoke(envelope.SenderNode, envelope.Token, payload);
                        message = new NetMQMessage();
                    }
                };

                retryTimer.Elapsed += (sender, args) =>
                {
                    var now = DateTimeOffset.UtcNow;
                    foreach (var pair in pending.ToArray())
                    {
                        if (pair.Value.NextAttemptUtc > now)
                        {
                            continue;
                        }

                        if (pair.Value.AttemptCount >= 5)
                        {
                            pending.Remove(pair.Key);
                            this._log(string.Format("[reliable] giving up on {0} to {1}", pair.Value.Envelope.MessageId, pair.Value.PeerNodeId));
                            continue;
                        }

                        DealerSocket dealer;
                        if (!dealers.TryGetValue(pair.Value.PeerNodeId, out dealer))
                        {
                            pending.Remove(pair.Key);
                            continue;
                        }

                        dealer.SendFrame(pair.Value.EncodedEnvelope);
                        pair.Value.AttemptCount++;
                        pair.Value.NextAttemptUtc = now.AddMilliseconds(800);
                        this._log(string.Format("[reliable] retry {0} to {1} (attempt {2})", pair.Value.Envelope.MessageId, pair.Value.PeerNodeId, pair.Value.AttemptCount));
                    }
                };

                this._poller = new NetMQPoller { this._commandQueue, router, retryTimer };
                this._started.Set();
                this._poller.Run();
            }
        }

        private static void SendReliableMessage(
            string peerNodeId,
            DealerSocket dealer,
            TransportEnvelope envelope,
            byte[] encodedEnvelope,
            IDictionary<string, PendingReliableMessage> pending)
        {
            dealer.SendFrame(encodedEnvelope);
            pending[BuildPendingKey(peerNodeId, envelope.MessageId)] = new PendingReliableMessage
            {
                PeerNodeId = peerNodeId,
                Envelope = envelope,
                EncodedEnvelope = encodedEnvelope,
                AttemptCount = 1,
                NextAttemptUtc = DateTimeOffset.UtcNow.AddMilliseconds(800)
            };
        }

        private static string BuildPendingKey(string peerNodeId, string messageId)
        {
            return peerNodeId + "::" + messageId;
        }

        private enum ReliableCommandKind
        {
            Broadcast,
            Connect,
            Disconnect,
            Stop
        }

        private sealed class ReliableCommand
        {
            public ReliableCommandKind Kind { get; private set; }

            public string Token { get; private set; }

            public TransportPayload Payload { get; private set; }

            public PeerNode Peer { get; private set; }

            public static ReliableCommand CreateBroadcast(string token, TransportPayload payload)
            {
                return new ReliableCommand { Kind = ReliableCommandKind.Broadcast, Token = token, Payload = payload };
            }

            public static ReliableCommand CreateConnect(PeerNode peer)
            {
                return new ReliableCommand { Kind = ReliableCommandKind.Connect, Peer = peer };
            }

            public static ReliableCommand CreateDisconnect(PeerNode peer)
            {
                return new ReliableCommand { Kind = ReliableCommandKind.Disconnect, Peer = peer };
            }

            public static ReliableCommand Stop()
            {
                return new ReliableCommand { Kind = ReliableCommandKind.Stop };
            }
        }

        private sealed class PendingReliableMessage
        {
            public string PeerNodeId { get; set; }

            public TransportEnvelope Envelope { get; set; }

            public byte[] EncodedEnvelope { get; set; }

            public int AttemptCount { get; set; }

            public DateTimeOffset NextAttemptUtc { get; set; }
        }
    }
}
