using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace MinHashSharp.Tests;

[TestClass]
public class MinHashIndexTests
{
    [TestMethod]
    public void MinHashLSHTest()
    {
        var lsh = CreateIndex();
        var hashes = GetTestHashes();
        for (int i = 0; i < hashes.Length; i++)
        {
            lsh.Add(T(i), hashes[i]);
        }

        static int T(int i) => i;

        HashSet<int> result = lsh.GetNearDupsOf(hashes[2]);
        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(1, result.Count);
        result = lsh.GetNearDupsOf(hashes[1]);
        Assert.IsTrue(result.Distinct().SequenceEqual([1, 3]));
    }

    [TestMethod]
    public void JaccardTest()
    {
        var hashes = GetTestHashes();
        Assert.AreEqual(1, hashes[0].GetApproximateJaccard(hashes[0]));
        Assert.AreEqual(0.3671875, hashes[0].GetApproximateJaccard(hashes[1]));
        Assert.AreEqual(0, hashes[0].GetApproximateJaccard(hashes[2]));
        Assert.AreEqual(0.375, hashes[0].GetApproximateJaccard(hashes[3]));
    }

    [TestMethod]
    public void SerializationTest()
    {
        MinHash[] hashes = GetRandomHashes(7);

        var lsh = CreateIndex();
        for (int i = 0; i < hashes.Length; i++)
        {
            lsh.Add(i + 1, hashes[i]);
        }

        using MemoryStream ms = new();
        lsh.Save(ms, KeySerializer, new Progress<int>(a =>
        {
            Debug.WriteLine($"{a}%");
            Console.Out.Flush();
        }));

        ms.Position = 0;
        var lsh2 = MinHashIndex<int>.Load(ms, KeyDeserializer, false);

        Assert.IsTrue(Array.TrueForAll(hashes, hash => lsh.GetNearDupsOf(hash).SequenceEqual(lsh2.GetNearDupsOf(hash))));
    }

    #region Static

    private static MinHashIndex<int> CreateIndex(double threshold = .8, int numPerm = 128, double fp = 0.5, double fn = 0.5)
        => new(threshold, numPerm, (fp, fn));

    private static MinHash[] GetTestHashes(int numPerm = 128)
    {
        string[] Text1 = "How are you? I Am fine. blar blar blar blar blar Thanks.".Split();
        string[] Text2 = "How are you i am fine. blar blar blar blar blar than".Split();
        string[] Text3 = "This is minhash test.".Split();
        string[] Text4 = "How are you i am fine. blar blar blar blar blar thank1".Split();

        return [
            new MinHash(numPerm).Update(Text1),
            new MinHash(numPerm).Update(Text2),
            new MinHash(numPerm).Update(Text3),
            new MinHash(numPerm).Update(Text4)];
    }

    private static MinHash[] GetRandomHashes(int count = 1000)
    {
        return Enumerable.Range(0, count)
           .AsParallel()
           .Select(i =>
           {
               return new MinHash(128).Update(Enumerable.Range(0, 100).Select(a =>
               {
                   byte[] bytes = ArrayPool<byte>.Shared.Rent(52);
                   try
                   {
                       Random.Shared.NextBytes(bytes);
                       return new string(MemoryMarshal.Cast<byte, char>(bytes));
                   }
                   finally
                   {
                       ArrayPool<byte>.Shared.Return(bytes);
                   }
               }).ToArray());
           }).ToArray();
    }

    private static void KeySerializer(BinaryWriter bw, int key) => bw.Write(key);

    private static int KeyDeserializer(BinaryReader br) => br.ReadInt32();

    #endregion
}