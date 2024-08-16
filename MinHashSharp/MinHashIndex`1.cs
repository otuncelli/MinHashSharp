using MathNet.Numerics.Integration;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Timer = System.Timers.Timer;

namespace MinHashSharp;

public class MinHashIndex<TKey> where TKey : notnull, IEquatable<TKey>
{
    private readonly int _numPerm, _numBuckets, _bucketRange;
    private readonly Dictionary<ulong, HashSet<TKey>>[] _hashTables;
    private readonly (int start, int end)[] _hashRanges;
    private readonly HashSet<TKey> _keys;
    private bool _frozen;

    /// <summary>
    /// Gets the number of keys in the index.
    /// </summary>
    public int Count => _keys.Count;

    /// <summary>
    /// Gets a value whether indicating the <see cref="MinHashIndex{TKey}"/> is frozen.
    /// </summary>
    public bool IsFrozen => _frozen;

    /// <summary>
    /// The `MinHash LSH` index. 
    /// Supporting for big-data fast querying using an approximate `Jaccard similarity` threshold 
    /// </summary>
    /// <param name="threshold">The Jaccard similarity threshold between 0.0 and 1.0. The initialized MinHash LSH will optimize the bucket count and ranges to minimize the false positives and false negatives.</param>
    /// <param name="numPerm">The number of permutation functions used by the MinHash to be indexed.</param>
    public MinHashIndex(double threshold, int numPerm) : this(threshold, numPerm, (0.5, 0.5))
    {
    }

    /// <summary>
    /// The `MinHash_LSH` index. 
    /// Supporting for big-data fast querying using an approximate `Jaccard similarity` threshold 
    /// </summary>
    /// <param name="threshold">The Jaccard similarity threshold between 0.0 and 1.0. The initialized MinHash LSH will optimize the bucket count and ranges to minimize the false positives and false negatives.</param>
    /// <param name="numPerm">The number of permutation functions used by the MinHash to be indexed.</param>
    /// <param name="weights">The weights to apply to the bucket optimization for fp (false-positive) and fn (false-negative) matches. Defaults to (0.5, 0.5)</param>
    public MinHashIndex(double threshold, int numPerm, (double fp, double fn) weights)
        : this(numPerm, CalculateOptimalBucketParams(threshold, numPerm, weights))
    {
    }

    private MinHashIndex(int numPerm, (int numBuckets, int bucketRange) param)
    {
        if (numPerm < 2)
            throw new ArgumentOutOfRangeException(nameof(numPerm), "Must be >=2");
        if (param.numBuckets * param.bucketRange > numPerm)
            throw new ArgumentOutOfRangeException(nameof(param), "Product of bucket * range must be less than numPerm");
        if (param.numBuckets < 2)
            throw new ArgumentOutOfRangeException(nameof(param), "param.numBuckets must be >=2");

        _numPerm = numPerm;
        _numBuckets = param.numBuckets;
        _bucketRange = param.bucketRange;
        _keys = [];
        _hashTables = new Dictionary<ulong, HashSet<TKey>>[_numBuckets];
        _hashRanges = new (int, int)[_numBuckets];
        for (int i = 0; i < _numBuckets; i++)
        {
            _hashTables[i] = [];
            _hashRanges[i] = (i * _bucketRange, (i + 1) * _bucketRange);
        }
    }

    /// <summary>
    /// Add a key to the index, together with a MinHash of the set referenced by a unique key.
    /// A list of keys will be the return value when querying for matches. 
    /// </summary>
    /// <param name="key">A unique key representing the hash</param>
    /// <param name="mh">The MinHash object</param>
    /// <exception cref="ArgumentException"></exception>
    public void Add(TKey key, MinHash mh)
    {
        if (_frozen)
            throw new InvalidOperationException("Cannot insert new hashes into a frozen index.");
        if (mh.Length != _numPerm)
            throw new ArgumentException("Permutation length of minhash doesn't match expected.", nameof(mh));
        if (_keys.Contains(key))
            throw new ArgumentException("Key already exists", nameof(key));

        _keys.Add(key);
        // Calculate the representation of each hash range and index them
        for (int i = 0; i < _numBuckets; i++)
        {
            (int start, int end) = _hashRanges[i];
            ulong h = CreateRepresentationOfHashValues(mh.GetHashValues(start, end));

            if (!_hashTables[i].TryGetValue(h, out HashSet<TKey>? value))
            {
                value = [];
                _hashTables[i].Add(h, value);
            }
            value.Add(key);
        } // next bucket
    }

    /// <summary>
    /// Removes a key from the index.
    /// </summary>
    /// <param name="key">A unique key representing the hash</param>
    public void Remove(TKey key)
    {
        _keys.Remove(key);

        for (int i = 0; i < _numBuckets; i++)
        {
            List<ulong> toRemove = [];
            foreach ((ulong hash, HashSet<TKey> bucket) in _hashTables[i])
            {
                bucket.Remove(key);
                if (bucket.Count == 0)
                {
                    toRemove.Add(hash);
                }
                bucket.TrimExcess();
            }
            toRemove.ForEach(s => _hashTables[i].Remove(s));
        } // next bucket

        _keys.TrimExcess();
    }

    /// <summary>
    /// Removes every entry that have been added to the index.
    /// </summary>
    public void Clear()
    {
        Array.ForEach(_hashTables, ht => 
        { 
            ht.Clear(); 
            ht.TrimExcess(); 
        });
        _keys.Clear();
        _keys.TrimExcess();
    }

    /// <summary>
    /// Giving the <see cref="MinHash" /> of the query set, retrieve the keys that 
    /// reference sets with Jaccard similarities likely greater than the threshold.
    /// Results are based on minhash segment collision and are thus approximate.
    /// For more accurate results, filter again with: `<see cref="MinHash.GetApproximateJaccard(MinHash)"/>`.
    /// </summary>
    /// <param name="mh">The MinHash object to find approximate matches for</param>
    /// <returns>
    /// An enumerable of `keys` that reference <see cref="MinHash" /> objects 
    /// that have a Jaccard similarity >= threshold (approximate)
    /// </returns>
    /// <exception cref="ArgumentException"></exception>
    public HashSet<TKey> GetNearDupsOf(MinHash mh)
    {
        ArgumentNullException.ThrowIfNull(mh);
        if (mh.Length != _numPerm)
            throw new ArgumentException("Permutation length of minhash doesn't match expected.", nameof(mh));

        HashSet<TKey> keys = [];

        // Calculate the representation of each hash range and check for matches in our table
        for (int i = 0; i < _numBuckets; i++)
        {
            ulong h = CreateRepresentationOfHashValues(mh.GetHashValues(_hashRanges[i].start, _hashRanges[i].end));

            if (_hashTables[i].TryGetValue(h, out HashSet<TKey>? value))
                foreach (TKey key in value)
                    keys.Add(key);
        } // next bucket

        return keys;
    }

    /// <summary>
    /// Freeze the LSH so that no more keys can be added, and by doing so we can clear out the keys array and save memory.
    /// </summary>
    public void Freeze()
    {
        _frozen = true;
        // Clear out the keys set, no need to store them anymore
        _keys.Clear();
        _keys.TrimExcess();
    }

    /// <summary>
    /// Serializes this <see cref="MinHashIndex{TKey}" /> index into a file.
    /// </summary>
    /// <param name="stream">The output <see cref="Stream" /> to save the index</param>
    /// <param name="keySerializer">Serialize callback to serialize T</param>
    /// <param name="progress">Progress reporting</param>
    public void Save(Stream stream, Action<BinaryWriter, TKey> keySerializer, IProgress<int>? progress = null)
    {
        ArgumentNullException.ThrowIfNull(keySerializer);
        ArgumentNullException.ThrowIfNull(stream);

        using ProgressThrottler? prog = progress is null ? null : new ProgressThrottler(progress, 1000d);
        double total = _hashTables.SelectMany(n => n.Values).Sum(h => h.Count);
        prog?.Report(0);

        using BinaryWriter bw = new(stream, Encoding.UTF8, leaveOpen: true);

        // Write out the 3 parameters
        bw.Write(_frozen);
        bw.Write(_numPerm);
        bw.Write(_numBuckets);
        bw.Write(_bucketRange);
        // The _hashRanges is generated automatically from these parameters, so no need to serialize it
        // Save all the keys 
        bw.Write(_keys.Count);
        foreach (TKey key in _keys)
            keySerializer(bw, key);
        // Save the _hashTables - count is _numBuckets
        for (int i = 0, j = 0; i < _numBuckets; i++)
        {
            bw.Write(_hashTables[i].Count);
            foreach (var kvp in _hashTables[i])
            {
                bw.Write(kvp.Key);
                bw.Write(kvp.Value.Count);
                foreach (TKey v in kvp.Value)
                {
                    keySerializer(bw, v);
                    prog?.Report((int)(++j / total * 100));
                }
            }
        }
        prog?.Report(100);
    }

    /// <summary>
    /// Serializes this <see cref="MinHashIndex{TKey}" /> index into a file.
    /// </summary>
    /// <param name="path">Path to save the index</param>
    /// <param name="keySerializer">Serialize callback to serialize T</param>
    /// <param name="progress">Progress reporting</param>
    public void Save(string path, Action<BinaryWriter, TKey> keySerializer, IProgress<int>? progress = null)
    {
        ArgumentNullException.ThrowIfNull(keySerializer);
        ArgumentNullException.ThrowIfNull(path);

        using FileStream stream = File.Open(path, FileMode.Create, FileAccess.Write, FileShare.Write);
        Save(stream, keySerializer, progress);
    }

    /// <summary>
    /// Deserialize an LSH index from a stream.
    /// </summary>
    /// <param name="stream">The input stream to load the index.</param>
    /// <param name="keyDeserializer">The callback to deserialize TKey.</param>
    /// <param name="forceFrozen">Whether to treat the index as frozen, and not load in the key array.</param>
    /// <returns>The loaded LSH index.</returns>
    public static MinHashIndex<TKey> Load(Stream stream, Func<BinaryReader, TKey> keyDeserializer, bool forceFrozen = false, IProgress<int>? progress = null)
    {
        ArgumentNullException.ThrowIfNull(keyDeserializer);
        ArgumentNullException.ThrowIfNull(stream);

        using ProgressThrottler? prog = progress is null ? null : new ProgressThrottler(progress, 1000d);
        prog?.Report(0);
        Debug.WriteLine("Deserializing LSH from stream...");
        using BinaryReader br = new(stream);

        // Read the first 3 parameters
        forceFrozen = br.ReadBoolean() || forceFrozen;
        int numPerm = br.ReadInt32();
        int numBuckets = br.ReadInt32();
        int bucketRange = br.ReadInt32();
        // Create our LSH object
        var lsh = new MinHashIndex<TKey>(numPerm, (numBuckets, bucketRange));

        Debug.WriteLine("Loading in keys set:");
        // Read in the keys
        int count = br.ReadInt32();
        int j = 0;
        while (count-- > 0)
        {
            long pos = stream.Position;
            TKey key = keyDeserializer(br);
            if (forceFrozen)
            {
                // skip reading the next keys if frozen
                pos = (stream.Position - pos) * count;
                stream.Seek(pos, SeekOrigin.Current);
                break;
            }
            else
            {
                lsh._keys.Add(key);
                prog?.Report((int)(++j * 25d / count));
            }
        }

        prog?.Report(25);
        Debug.WriteLine($"Reading in {numBuckets} buckets:");
        // Read in the _hashTables
        // Save the _hashTables - count is _numBuckets
        for (int i = 0; i < numBuckets; i++)
        {
            count = br.ReadInt32();
            while (count-- > 0)
            {
                // Key: ulong, Value: HashSet<TKey>
                ulong key = br.ReadUInt64();
                var value = new HashSet<TKey>();
                int valueCount = br.ReadInt32();
                while (valueCount-- > 0)
                    value.Add(keyDeserializer(br));
                // Add it in to our hashTables object
                lsh._hashTables[i][key] = value;
            }
            prog?.Report((int)(i * 75d / numBuckets));
        }

        prog?.Report(100);
        Debug.WriteLine("Finished reading buckets.");
        return lsh;
    }

    /// <summary>
    /// Deserialize an LSH index from a file
    /// </summary>
    /// <param name="path">The path to the index file.</param>
    /// <param name="keyDeserializer">The callback to deserialize the key.</param>
    /// <param name="frozen">Whether to treat the index as frozen, and not load in the key array.</param>
    /// <returns>The loaded LSH index</returns>
    public static MinHashIndex<TKey> Load(string path, Func<BinaryReader, TKey> keyDeserializer, bool frozen = false)
    {
        ArgumentNullException.ThrowIfNull(keyDeserializer);
        ArgumentNullException.ThrowIfNull(path);

        using FileStream stream = File.OpenRead(path);
        return Load(stream, keyDeserializer, frozen);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong CreateRepresentationOfHashValues(ReadOnlySpan<uint> vals)
    {
        // Cast uints to bytes, and re-hash.
        // Should be unique enough and the memory fingerprint is smaller
        return Farmhash.Sharp.Farmhash.Hash64(MemoryMarshal.Cast<uint, byte>(vals));
    }

    private static (int b, int r) CalculateOptimalBucketParams(double threshold, int numPerm, (double fp, double fn) weights)
    {
        // Validations:
        if (threshold is > 1 or < 0)
            throw new ArgumentOutOfRangeException(nameof(threshold), "Must be in [0.0, 1.0]");
        if (weights.fn is < 0 or > 1 || weights.fp is < 0 or > 1)
            throw new ArgumentOutOfRangeException(nameof(weights), "Must be in [0.0, 1.0]");
        if (Math.Round(weights.fn + weights.fp, 1) != 1.0)
            throw new ArgumentOutOfRangeException(nameof(weights), "Must sum to 1.0");

        double minError = double.MaxValue;
        var opt = (0, 0);// optimal value to return, initialize to zeros

        // Figure out the optimal way to choose the number buckets and the bucket range
        // Go through and calculate the error for each permutation
        for (int b = 1; b <= numPerm; b++)
        {
            int maxR = numPerm / b;
            for (int r = 1; r <= maxR; r++)
            {
                // Calculate the false positive and negative probabilities by integrating over the
                // error function for each, and choose the set that produces the smallest error.
                // fp = f(x) = 1 - ((1 - x^r)^b), integrated from 0 to thresh
                var fpProb = GaussLegendreRule.Integrate(x => 1 - Math.Pow(1 - Math.Pow(x, r), b), invervalBegin: 0, invervalEnd: threshold, order: 5);
                // fn = f(x) = 1 - fp(x), integrated from thresh to 1
                var fnProb = GaussLegendreRule.Integrate(x => Math.Pow(1 - Math.Pow(x, r), b), invervalBegin: threshold, invervalEnd: 1, order: 5);

                // Combine the probabilities using the weights that were given (e.g., one can prioritize
                // having fewer false positives at the cost of false negatives)
                var error = fpProb * weights.fp + fnProb * weights.fn;
                if (error < minError)
                {
                    opt = (b, r);
                    minError = error;
                }
            } // next range size
        } // next bucket size

        return opt;
    }

    /// <summary>
    /// <see cref="IProgress{T}" /> implementation with throttling based on a <see cref="Timer" />.
    /// </summary>
    private sealed class ProgressThrottler : IProgress<int>, IDisposable
    {
        private readonly IProgress<int> _progress;
        private readonly Timer _timer;
        private bool _canReport = true;

        /// <summary>
        /// <see cref="IProgress{T}" /> implementation throttling based on <see cref="Timer">.
        /// </summary>
        public ProgressThrottler(IProgress<int> progress, double interval)
        {
            _progress = progress ?? throw new ArgumentNullException(nameof(progress));
            _timer = new Timer(interval);
            _timer.Elapsed += (s, e) => _canReport = true;
            _timer.Start();
        }

        public void Report(int value)
        {
            // Throttles the amount of calls
            if (_canReport)
            {
                _progress.Report(value);
                _canReport = false;
            }
        }

        public void Dispose()
        {
            _timer.Dispose();
        }
    }
}