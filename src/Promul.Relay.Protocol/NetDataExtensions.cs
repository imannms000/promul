using System;
using System.Diagnostics;

namespace Promul.Relay.Protocol
{
    public static class NetDataExtensions
    {
        public static RelayControlMessage ReadRelayControlMessage(this CompositeReader reader)
        {
            var rcm = new RelayControlMessage
            {
                Type = (RelayControlMessageType)reader.ReadByte(),
                AuthorClientId = reader.ReadUInt64(),
            };

            // Read the JoinCode length and then the JoinCode bytes
            int joinCodeLength = reader.ReadInt32(); // Read the length of JoinCode as int
            if (joinCodeLength > 0)
            {
                rcm.JoinCode = reader.ReadBytes(joinCodeLength); // Read the JoinCode bytes
            }
            else
            {
                rcm.JoinCode = Array.Empty<byte>(); // Use an empty array if length is 0
            }

            rcm.Data = reader.ReadRemainingBytes(); // Read the additional data
            return rcm;
        }

        public static void Write(this CompositeWriter writer, RelayControlMessage rcm)
        {
            Debug.Write($"[netcode] NetDataExtensions # Write # rcm.JoinCode: {rcm.JoinCode}");
            writer.Write((byte)rcm.Type);
            writer.Write(rcm.AuthorClientId);
            // Write the JoinCode length followed by the JoinCode bytes
            writer.Write(rcm.JoinCode?.Length ?? 0); // Write the length of JoinCode
            if (rcm.JoinCode != null)
            {
                writer.Write(rcm.JoinCode); // Write the JoinCode bytes
            }
            writer.Write(rcm.Data);
        }
    }
}