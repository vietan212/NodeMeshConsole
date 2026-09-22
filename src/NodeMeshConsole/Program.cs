using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using NetMQ;

namespace NodeMeshConsole
{
    internal static class Program
    {
        private static int Main(string[] args)
        {
            NodeIdentity nodeIdentity;
            if (!TryParseNodeIdentity(args, out nodeIdentity))
            {
                PrintUsage();
                return 1;
            }

            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                eventArgs.Cancel = true;
            };

            try
            {
                using (var nodeTransport = new NodeTransport(nodeIdentity, Log))
                {
                    nodeTransport.Start();
                    PrintHelp();

                    while (true)
                    {
                        Console.Write("> ");
                        var line = Console.ReadLine();
                        if (line == null)
                        {
                            break;
                        }

                        if (!TryHandleCommand(nodeTransport, line))
                        {
                            break;
                        }
                    }
                }

                return 0;
            }
            finally
            {
                NetMQConfig.Cleanup(false);
            }
        }

        private static bool TryHandleCommand(NodeTransport nodeTransport, string line)
        {
            var trimmed = (line ?? string.Empty).Trim();
            if (trimmed.Length == 0)
            {
                return true;
            }

            if (string.Equals(trimmed, "quit", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(trimmed, "exit", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            if (string.Equals(trimmed, "help", StringComparison.OrdinalIgnoreCase))
            {
                PrintHelp();
                return true;
            }

            if (string.Equals(trimmed, "peers", StringComparison.OrdinalIgnoreCase))
            {
                foreach (var peer in nodeTransport.Peers)
                {
                    Log(peer.ToString());
                }

                if (!nodeTransport.Peers.Any())
                {
                    Log("No peers discovered.");
                }

                return true;
            }

            if (string.Equals(trimmed, "latest", StringComparison.OrdinalIgnoreCase))
            {
                foreach (var latest in nodeTransport.LatestStreamValues)
                {
                    Log(string.Format(
                        "{0}::{1} @ {2:O} => {3}",
                        latest.SenderNode,
                        latest.Token,
                        latest.ReceivedAt,
                        PayloadCodec.Describe(latest.Payload)));
                }

                if (!nodeTransport.LatestStreamValues.Any())
                {
                    Log("No stream values received yet.");
                }

                return true;
            }

            string channel;
            string payloadKind;
            string token;
            string payloadArgument;
            if (!TrySplitPublishCommand(trimmed, out channel, out payloadKind, out token, out payloadArgument))
            {
                Log("Invalid command. Type 'help' for usage.");
                return true;
            }

            TransportPayload payload;
            try
            {
                payload = ParsePayload(payloadKind, payloadArgument);
            }
            catch (Exception exception)
            {
                Log("Payload parse error: " + exception.Message);
                return true;
            }

            if (string.Equals(channel, "stream", StringComparison.OrdinalIgnoreCase))
            {
                nodeTransport.PublishStream(token, payload);
                return true;
            }

            if (string.Equals(channel, "reliable", StringComparison.OrdinalIgnoreCase))
            {
                nodeTransport.BroadcastReliable(token, payload);
                return true;
            }

            Log("Unknown channel '" + channel + "'.");
            return true;
        }

        private static TransportPayload ParsePayload(string payloadKind, string payloadArgument)
        {
            switch (payloadKind.ToLowerInvariant())
            {
                case "double":
                    return PayloadCodec.FromDouble(ParseDouble(payloadArgument));
                case "double[]":
                case "doubles":
                case "array":
                    return PayloadCodec.FromDoubleArray(payloadArgument.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries).Select(ParseDouble));
                case "bytes":
                case "buffer":
                    return PayloadCodec.FromGeneratedBuffer(int.Parse(payloadArgument, CultureInfo.InvariantCulture));
                case "kv":
                case "map":
                case "pairs":
                    return PayloadCodec.FromKeyValueSet(ParseKeyValuePairs(payloadArgument));
                default:
                    throw new InvalidOperationException("Unsupported payload kind '" + payloadKind + "'.");
            }
        }

        private static Dictionary<string, double> ParseKeyValuePairs(string value)
        {
            var result = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
            foreach (var pairText in value.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries))
            {
                var segments = pairText.Split(new[] { '=' }, 2);
                if (segments.Length != 2)
                {
                    throw new InvalidOperationException("Key/value payloads must use key=value pairs separated by commas.");
                }

                result[segments[0].Trim()] = ParseDouble(segments[1].Trim());
            }

            return result;
        }

        private static double ParseDouble(string value)
        {
            return double.Parse(value.Trim(), CultureInfo.InvariantCulture);
        }

        private static bool TrySplitPublishCommand(string line, out string channel, out string payloadKind, out string token, out string payloadArgument)
        {
            channel = null;
            payloadKind = null;
            token = null;
            payloadArgument = null;

            var firstSpace = line.IndexOf(' ');
            if (firstSpace < 0)
            {
                return false;
            }

            channel = line.Substring(0, firstSpace).Trim();
            var remainder = line.Substring(firstSpace + 1).Trim();

            var secondSpace = remainder.IndexOf(' ');
            if (secondSpace < 0)
            {
                return false;
            }

            payloadKind = remainder.Substring(0, secondSpace).Trim();
            remainder = remainder.Substring(secondSpace + 1).Trim();

            var thirdSpace = remainder.IndexOf(' ');
            if (thirdSpace < 0)
            {
                return false;
            }

            token = remainder.Substring(0, thirdSpace).Trim();
            payloadArgument = remainder.Substring(thirdSpace + 1).Trim();
            return token.Length > 0 && payloadArgument.Length > 0;
        }

        private static bool TryParseNodeIdentity(string[] args, out NodeIdentity nodeIdentity)
        {
            nodeIdentity = default(NodeIdentity);
            if (args == null || args.Length != 1)
            {
                return false;
            }

            return Enum.TryParse(args[0], true, out nodeIdentity) && Enum.IsDefined(typeof(NodeIdentity), nodeIdentity);
        }

        private static void PrintUsage()
        {
            Console.WriteLine("Usage: NodeMeshConsole <AP|RU|RV>");
        }

        private static void PrintHelp()
        {
            Console.WriteLine("Commands:");
            Console.WriteLine("  stream double <token> <value>");
            Console.WriteLine("  stream doubles <token> <v1,v2,v3>");
            Console.WriteLine("  stream bytes <token> <length>");
            Console.WriteLine("  stream kv <token> <key1=value1,key2=value2>");
            Console.WriteLine("  reliable double <token> <value>");
            Console.WriteLine("  reliable doubles <token> <v1,v2,v3>");
            Console.WriteLine("  reliable bytes <token> <length>");
            Console.WriteLine("  reliable kv <token> <key1=value1,key2=value2>");
            Console.WriteLine("  peers");
            Console.WriteLine("  latest");
            Console.WriteLine("  help");
            Console.WriteLine("  quit");
        }

        private static void Log(string message)
        {
            Console.WriteLine("[{0:HH:mm:ss}] {1}", DateTime.Now, message);
        }
    }
}
