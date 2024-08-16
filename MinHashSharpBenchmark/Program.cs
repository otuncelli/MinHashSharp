using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using MinHashSharp;
using System.Buffers;
using System.Runtime.InteropServices;

namespace MinHashSharpBenchmark
{
    [MemoryDiagnoser(false)]
    public class Md5VsSha256
    {
        private const int N = 10000;
        private readonly byte[] data;

        private readonly MinHash[] Hashes = GetRandomHashes(20000);

        public Md5VsSha256()
        {
            data = new byte[N];
            new Random(42).NextBytes(data);
        }

        [Benchmark]
        public void MinHashLSH()
        {
            var lsh = CreateIndex();
            for (int i = 0; i < Hashes.Length; i++)
            {
                lsh.Add(i + 1, Hashes[i]);
            }
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

        private static MinHashIndex<int> CreateIndex(double threshold = .8, int numPerm = 128, double fp = 0.5, double fn = 0.5)
            => new(threshold, numPerm, (fp, fn));
    }

    internal class Program
    {
        static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<Md5VsSha256>();
        }
    }
}
