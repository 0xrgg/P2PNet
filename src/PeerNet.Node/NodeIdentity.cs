using System.Buffers;
using System.Security.Cryptography;
using System.Text;

namespace PeerNet.Node;

/// <summary>
/// This process's node identity, shared through DI. The NodeId is derived from a persisted ECDSA P-256 key,
/// so it stays the same across restarts.
/// </summary>
public sealed class NodeIdentity
{
    private NodeIdentity(byte[] privateKey, byte[] publicKey)
    {
        PrivateKey = privateKey;
        PublicKey = publicKey;
        NodeId = DeriveNodeId(publicKey);

        _signer = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        _signer.ImportPkcs8PrivateKey(privateKey, out _);

    }

    public Guid NodeId { get; }

    /// <summary>The private key, PKCS#8 encoded. Load it with <see cref="ECDsa.ImportPkcs8PrivateKey"/>; never send it to peers.</summary>
    public byte[] PrivateKey { get; }

    /// <summary>The public key, SubjectPublicKeyInfo encoded. Safe to share; peers check that it hashes to <see cref="NodeId"/>.</summary>
    public byte[] PublicKey { get; }

    /// <summary>The first 16 bytes of SHA-256 over a SubjectPublicKeyInfo public key.</summary>
    public static Guid DeriveNodeId(ReadOnlySpan<byte> publicKey) => new(SHA256.HashData(publicKey).AsSpan(0, 16));

    /// <summary> Used to sign the attestation data </summary>
    private readonly ECDsa _signer;

    private readonly object _lock = new();

    /// <summary>A fresh in-memory key that is never persisted. Intended for tests.</summary>
    public static NodeIdentity CreateNew()
    {
        using var ecdsa = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        return new NodeIdentity(ecdsa.ExportPkcs8PrivateKey(), ecdsa.ExportSubjectPublicKeyInfo());
    }

    /// <summary>
    /// Loads the PKCS#8 private key at <paramref name="privateKeyPath"/>, or creates and saves a new one if it does not exist.
    /// The public key is always derived from the private key; <paramref name="publicKeyPath"/> is written for reference only.
    /// </summary>
    public static NodeIdentity LoadOrCreate(string publicKeyPath, string privateKeyPath)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(privateKeyPath))!);
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(publicKeyPath))!);

        using var ecdsa = ECDsa.Create(ECCurve.NamedCurves.nistP256);

        byte[] privateKey;

        if (File.Exists(privateKeyPath))
        {
            privateKey = File.ReadAllBytes(privateKeyPath);
            ecdsa.ImportPkcs8PrivateKey(privateKey, out _);
        }
        else
        {
            // A new ECDsa already holds a fresh key
            privateKey = ecdsa.ExportPkcs8PrivateKey();
            WriteAtomically(privateKeyPath, privateKey);
        }

        var publicKey = ecdsa.ExportSubjectPublicKeyInfo();

        // Keep the public key file in step with the private key, e.g. if it was deleted or is stale
        if (!File.Exists(publicKeyPath) || !File.ReadAllBytes(publicKeyPath).AsSpan().SequenceEqual(publicKey))
        {
            WriteAtomically(publicKeyPath, publicKey);
        }

        return new NodeIdentity(privateKey, publicKey);
    }

    // Write via a temp file so a crash can't leave a partial key
    private static void WriteAtomically(string path, byte[] bytes)
    {
        var tmp = path + ".tmp";
        File.WriteAllBytes(tmp, bytes);
        File.Move(tmp, path, overwrite: true);
    }

    public byte[] Sign(ReadOnlySpan<byte> data)
    {
        lock (_lock)
        {
            return _signer.SignData(data, HashAlgorithmName.SHA256);
        }
    }

    public static bool Verify(ReadOnlySpan<byte> publicKey, ReadOnlySpan<byte> data, ReadOnlySpan<byte> signature)
    {
        try
        {
            using var ecdsa = ECDsa.Create();
            ecdsa.ImportSubjectPublicKeyInfo(publicKey, out _);
            return ecdsa.VerifyData(data, signature, HashAlgorithmName.SHA256);
        }
        catch (Exception ex)
        {
            return false;
        }

    }

    // Shared by the signer and the verifier
    public static byte[] BuildSigningPayload(Guid fileId, string checksum, Guid nodeId)
    {
        var buffer = new ArrayBufferWriter<byte>();
        buffer.Write("PeerNet.FileAttestation.v1"u8);
        buffer.Write(fileId.ToByteArray());
        buffer.Write(nodeId.ToByteArray());
        buffer.Write(Encoding.UTF8.GetBytes(checksum));   // variable-length field last
        return buffer.WrittenSpan.ToArray();
    }
}
