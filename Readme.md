# .NET Solution for One Billion Row Challenge

My blog post: https://hotforknowledge.com/2024/01/13/1brc-in-dotnet-among-fastest-on-linux-my-optimization-journey/

## Results

See [results](https://hotforknowledge.com/2024/01/13/1brc-in-dotnet-among-fastest-on-linux-my-optimization-journey/#results) in the blog post.

A separate repository for automated benchmarks: https://github.com/buybackoff/1brc-bench.

## Build & Run on Linux

To install .NET on Linux, follow [official instructions](https://learn.microsoft.com/en-us/dotnet/core/install/linux).

To build, run `./build.sh`. 

To run JIT version: `./jit.sh /path/to/measurements.txt`.

To run AOT version: `./aot.sh /path/to/measurements.txt`.