namespace MinHashSharp;

/// <summary>
/// MinHash is a probabilistic data structure for computing Jaccard similarity between sets.
/// </summary>
public class MinHash
{
    private readonly int _numPerm, _seed;
    private readonly uint[] _hashValues;
    private readonly Func<string, uint> _hashFunc;
    private readonly (ulong a, ulong b)[] _perms;
    private static readonly ReaderWriterLockSlim _lock = new();
    private static (int numPerm, int seed, (ulong a, ulong b)[] perms) _cachedPermutation;
    private const ulong MersennePrime = (1L << 61) - 1; // http://en.wikipedia.org/wiki/Mersenne_prime

    /// <summary>
    /// The number of permutations in this MinHash
    /// </summary>
    public int Length => _numPerm;

    /// <summary>
    /// MinHash is a probabilistic data structure for computing Jaccard similarity between sets.
    /// </summary>
    /// <param name="numPerm">Number of random permutation functions.</param>
    /// <param name="seed">The random seed for generating the permutation functions for this MinHash. MinHash's generated with different seeds aren't comparable.</param>
    /// <param name="hashFunc">The hash function to use. Defaults to Google's Farmhash</param>
    public MinHash(int numPerm, int seed = 1111, Func<string, uint>? hashFunc = null)
    {
        _numPerm = numPerm;
        _seed = seed;
        _hashValues = Enumerable.Repeat(uint.MaxValue, numPerm).ToArray();
        _hashFunc = hashFunc ?? Farmhash.Sharp.Farmhash.Hash32;
        _perms = InitPermutations(_numPerm, _seed);
    }

    /// <summary>
    /// Update this MinHash with more values from the current set that will be hashed.
    /// The value will be hashed using the hash function specified by the hashFunc parameter specified in the constructor.
    /// </summary>
    /// <param name="values">The values from the set to hash</param>
    /// <returns></returns>
    public MinHash Update(ReadOnlySpan<string> values)
    {
        // For each value, we calculate N hashes like this:
        // ((hash(val) * ai + bi) % MersennePrime)
        // Take the minimum for each one
        Span<uint> hashValues = _hashValues;
        ReadOnlySpan<(ulong a, ulong b)> perms = _perms;
        for (int i = 0; i < _numPerm; i++)
        {
            (ulong ai, ulong bi) = _perms[i];

            for (int iValue = 0; iValue < values.Length; iValue++)
            {
                uint hash = unchecked((uint)((_hashFunc(values[iValue]) * ai + bi) % MersennePrime));
                if (hash < hashValues[i])
                {
                    hashValues[i] = hash;
                }
            } // next value
        }

        return this; // to allow chaining
    }

    /// <summary>
    /// Estimate the Jaccard similarity between two sets represented by their MinHash object. 
    /// </summary>
    /// <param name="other">The MinHash representing the comparison set</param>
    /// <returns>The estimated Jaccard similarity, between 0.0 and 1.0</returns>
    /// <exception cref="ArgumentException">Comparison MinHash must have the same seed and number of permutations</exception>
    public double GetApproximateJaccard(MinHash other)
    {
        // Validation:
        if (other._seed != _seed)
            throw new ArgumentException("Cannot compute Jaccard given MinHash with different seeds");
        if (other._numPerm != _numPerm)
            throw new ArgumentException("Cannot compute Jaccard given MinHash with different number of permutation functions");

        // Jaccard similarity is defined as the number of intersect / count. 
        int c = 0;
        ReadOnlySpan<uint> _this = _hashValues;
        ReadOnlySpan<uint> _other = other._hashValues;
        for (int i = 0; i < _numPerm; i++)
        {
            if (_other[i] == _this[i])
            {
                c++;
            }
        }

        return c / (double)_numPerm;
    }

    /// <summary>
    /// Return a subset of the hash values in a specific range. Used by the MinHashLSH to index into buckets.
    /// </summary>
    /// <param name="start">Index of first hash value</param>
    /// <param name="end">Index of last hash value (exclusive upper-bound)</param>
    /// <returns></returns>
    public ReadOnlySpan<uint> GetHashValues(int start, int end)
    {
        return _hashValues.AsSpan()[start..Math.Min(end, Math.Min(end, _hashValues.Length))];
    }

    private static (ulong, ulong)[] InitPermutations(int numPerm, int seed)
    {
        _lock.EnterUpgradeableReadLock();
        try
        {
            // In case there's an update to the cache object from a different thread, just store the instance
            // locally, so the check will be safe. 
            var cached = _cachedPermutation;
            if (cached.numPerm == numPerm && cached.seed == seed)
            {
                return cached.perms;
            }

            _lock.EnterWriteLock();
            try
            {
                // Create parameters for a random bijective permutation function
                // that maps a 32-bit hash value to another 32-bit hash value.
                // http://en.wikipedia.org/wiki/Universal_hashing

                var r = new Random(seed);

                // Calculate the permtuations!
                (ulong a, ulong b)[] perms = new (ulong, ulong)[numPerm];

                for (int i = 0; i < numPerm; i++)
                {
                    perms[i] = (
                        unchecked((ulong)r.NextInt64(1, (long)MersennePrime)), 
                        unchecked((ulong)r.NextInt64(0, (long)MersennePrime)));
                } // next perm

                // Cache the result and return it
                _cachedPermutation = (numPerm, seed, perms);
                return perms;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
        finally
        {
            _lock.ExitUpgradeableReadLock();
        }
    }
}