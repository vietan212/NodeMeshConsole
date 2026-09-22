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
    internal sealed class StreamTransport : IDisposable
    {
        private readonly string _localNodeId;
        private readonly int _bindPort;
        private readonly Action<string> _log;
        private readonly NetMQQueue<StreamCommand> _commandQueue;
        private readonly ConcurrentDictionary<string, LatestStreamValue> _latestByToken;
        private readonly ManualResetEventSlim _started;
        private Thread _workerThread;
        private NetMQPoller _poller;

        public StreamTransport(string localNodeId, int bindPort, Action<string> log)
        {
            this._localNodeId = localNodeId;
            this._bindPort = bindPort;
            this._log = log;
            this._commandQueue = new NetMQQueue<StreamCommand>();
            this._latestByToken = new ConcurrentDictionary<string, LatestStreamValue>(StringComparer.OrdinalIgnoreCase);
            this._started = new ManualResetEventSlim(false);
        }

        public event Action<string, string, TransportPayload> MessageReceived;

        public IReadOnlyCollection<LatestStreamValue> LatestValues
        {
            get { return this._latestByToken.Values.OrderBy(value => value.Token, StringComparer.OrdinalIgnoreCase).ToArray(); }
        }

        public void Start()
        {
            this._workerThread = new Thread(this.RunWorker)
            {
                IsBackground = true,
                Name = "StreamTransport"
            };
            this._workerThread.Start();
            this._started.Wait(TimeSpan.FromSeconds(5));
        }

        public void Publish(string token, TransportPayload payload)
        {
            this._commandQueue.Enqueue(StreamCommand.CreatePublish(token, payload));
        }

        public void ConnectPeer(PeerNode peer)
        {
            this._commandQueue.Enqueue(StreamCommand.CreateConnect(peer));
        }

        public void DisconnectPeer(PeerNode peer)
        {
            this._commandQueue.Enqueue(StreamCommand.CreateDisconnect(peer));
        }

        public void Dispose()
        {
            this._commandQueue.Enqueue(StreamCommand.Stop());

            if (this._workerThread != null && this._workerThread.IsAlive)
            {
                this._workerThread.Join(TimeSpan.FromSeconds(2));
            }

            this._commandQueue.Dispose();
            this._started.Dispose();
        }

        private void RunWorker()
        {
            var pendingByToken = new Dictionary<string, TransportEnvelope>(StringComparer.OrdinalIgnoreCase);
            var connectedEndpointsByPeer = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            using (var publisher = new PublisherSocket())
            using (var subscriber = new SubscriberSocket())
            {
                var flushTimer = new NetMQTimer(TimeSpan.FromMilliseconds(100));
                publisher.Options.SendHighWatermark = 1000;
                publisher.Bind("tcp://*:" + this._bindPort);

                subscriber.Options.ReceiveHighWatermark = 1000;
                subscriber.SubscribeToAnyTopic();

                this._commandQueue.ReceiveReady += (sender, args) =>
                {
                    StreamCommand command;
                    while (args.Queue.TryDequeue(out command, TimeSpan.Zero))
                    {
                        if (command.Kind == StreamCommandKind.Stop)
                        {
                            this._poller.Stop();
                            return;
                        }

                        if (command.Kind == StreamCommandKind.Publish)
                        {
                            pendingByToken[command.Token] = EnvelopeCodec.CreateData(this._localNodeId, command.Token, command.Payload);
                            continue;
                        }

                        if (command.Peer == null)
                        {
                            continue;
                        }

                        var endpoint = "tcp://" + command.Peer.IpAddress + ":" + command.Peer.StreamPort;
                        if (command.Kind == StreamCommandKind.Connect)
                        {
                            string currentEndpoint;
                            if (connectedEndpointsByPeer.TryGetValue(command.Peer.NodeId, out currentEndpoint))
                            {
                                if (string.Equals(currentEndpoint, endpoint, StringComparison.OrdinalIgnoreCase))
                                {
                                    continue;
                                }

                                subscriber.Disconnect(currentEndpoint);
                            }

                            subscriber.Connect(endpoint);
                            connectedEndpointsByPeer[command.Peer.NodeId] = endpoint;
                            this._log(string.Format("[stream] connected SUB to {0}", endpoint));
                        }
                        else if (command.Kind == StreamCommandKind.Disconnect)
                        {
                            string currentEndpoint;
                            if (connectedEndpointsByPeer.TryGetValue(command.Peer.NodeId, out currentEndpoint))
                            {
                                subscriber.Disconnect(currentEndpoint);
                                connectedEndpointsByPeer.Remove(command.Peer.NodeId);
                                this._log(string.Format("[stream] disconnected SUB from {0}", currentEndpoint));
                            }
                        }
                    }
                };

                flushTimer.Elapsed += (sender, args) =>
                {
                    if (pendingByToken.Count == 0)
                    {
                        return;
                    }

                    foreach (var pair in pendingByToken.ToArray())
                    {
                        publisher.SendMoreFrame(Encoding.UTF8.GetBytes(pair.Key)).SendFrame(EnvelopeCodec.Encode(pair.Value));
                    }

                    pendingByToken.Clear();
                };

                subscriber.ReceiveReady += (sender, args) =>
                {
                    var message = new NetMQMessage();
                    while (args.Socket.TryReceiveMultipartMessage(ref message, 0))
                    {
                        if (message.FrameCount < 2)
                        {
                            message = new NetMQMessage();
                            continue;
                        }

                        var token = message[0].ConvertToString(Encoding.UTF8);
                        var envelope = EnvelopeCodec.Decode(message[1].ToByteArray());
                        if (envelope.Kind != EnvelopeKind.Data)
                        {
                            continue;
                        }

                        var payload = PayloadCodec.Decode(envelope.PayloadBytes);
                        this._latestByToken[token] = new LatestStreamValue
                        {
                            Token = token,
                            SenderNode = envelope.SenderNode,
                            ReceivedAt = DateTimeOffset.UtcNow,
                            Payload = payload
                        };

                        this.MessageReceived?.Invoke(envelope.SenderNode, token, payload);
                        message = new NetMQMessage();
                    }
                };

                this._poller = new NetMQPoller { this._commandQueue, subscriber, flushTimer };
                this._started.Set();
                this._poller.Run();
            }
        }

        private enum StreamCommandKind
        {
            Publish,
            Connect,
            Disconnect,
            Stop
        }

        private sealed class StreamCommand
        {
            public StreamCommandKind Kind { get; private set; }

            public string Token { get; private set; }

            public TransportPayload Payload { get; private set; }

            public PeerNode Peer { get; private set; }

            public static StreamCommand CreatePublish(string token, TransportPayload payload)
            {
                return new StreamCommand { Kind = StreamCommandKind.Publish, Token = token, Payload = payload };
            }

            public static StreamCommand CreateConnect(PeerNode peer)
            {
                return new StreamCommand { Kind = StreamCommandKind.Connect, Peer = peer };
            }

            public static StreamCommand CreateDisconnect(PeerNode peer)
            {
                return new StreamCommand { Kind = StreamCommandKind.Disconnect, Peer = peer };
            }

            public static StreamCommand Stop()
            {
                return new StreamCommand { Kind = StreamCommandKind.Stop };
            }
        }
    }
}
