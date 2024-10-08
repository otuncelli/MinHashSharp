# MinHashSharp - A Robust Library for Similarity Estimation

[![NuGet](https://img.shields.io/nuget/v/MinHashSharp.svg)](https://www.nuget.org/packages/MinHashSharp/)

`MinHashSharp` offers a simple lightweight data structure designed to index and estimate Jaccard similarity between sets. Leveraging its robust structure, it has been successfully tested on datasets as large as 60GB, encompassing tens of millions of documents, while ensuring smooth and efficient operations.

## Installation

To incorporate MinHashSharp into your project, choose one of the following methods:

### .NET CLI
```bash
dotnet add package MinHashSharp
```

### NuGet Package Manager
```powershell
Install-Package MinHashSharp
```

For detailed package information, visit [MinHashSharp on NuGet](https://www.nuget.org/packages/MinHashSharp/).

## Key Features

The library currently offers two classes:

`MinHash`: A probabilistic data structure for computing Jaccard similarity between sets. 

`MinHashLSH<TKey>`: A class for supporting big-data fast querying using an approximate `Jaccard similarity` threshold.

## Sample usage

```cs
string s1 = "The quick brown fox jumps over the lazy dog and proceeded to run towards the other room";
string s2 = "The slow purple elephant runs towards the happy fox and proceeded to run towards the other room";
string s3 = "The quick brown fox jumps over the angry dog and proceeded to run towards the other room";

var m1 = new MinHash(numPerm: 128).Update(s1.Split());
var m2 = new MinHash(numPerm: 128).Update(s2.Split());
var m3 = new MinHash(numPerm: 128).Update(s3.Split());

Console.WriteLine(m1.GetApproximateJaccard(m2));// 0.51

var lsh = new MinHashLSH<string>(threshold: 0.8, numPerm: 128);

lsh.Add("s1", m1);
lsh.Add("s2", m2);

Console.WriteLine(string.Join(", ", lsh.Query(m3))); // s1
```

## Multi-threading

The library is entirely thread-safe except for the `MinHashLSH<TKey>.Add` function (and the custom injected hash function, if relevant). Therefore, you can create `MinHash` objects on multiple threads and query the same `MinHashLSH<TKey>` object freely. If you are indexing sets on multiple threads, then just make sure to gain exclusive access to the LSH around every `Add` call:

```cs
lock (lsh) {
    lsh.Add("s3", m3);
}
```

## Custom hash function

By default, the library uses the [Farmhash function](https://opensource.googleblog.com/2014/03/introducing-farmhash.html) introduced by Google for efficiency. For more accurate hashes, one can inject a custom hash function into the `MinHash` object.

For example, if you want to use the C# default string hash function:

```cs
static uint StringHash(string s) => (uint)s.GetHashCode();

var m = new MinHash(numPerm: 128, hashFunc: StringHash).Update(s1.Split());
```
