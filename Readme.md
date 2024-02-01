# .NET Solution for One Billion Row Challenge

My blog post: https://hotforknowledge.com/2024/01/13/1brc-in-dotnet-among-fastest-on-linux-my-optimization-journey/

## Results

> Pay attentions to the dates shown in the tables. The results are old there. New numbers are WIP.

See [aggregated results](https://hotforknowledge.com/2024/01/13/1brc-in-dotnet-among-fastest-on-linux-my-optimization-journey/#results) in the blog post. Don't miss [a link to details avg/min/max/sigma results](https://hotforknowledge.com/2024/01/13/1brc-in-dotnet-among-fastest-on-linux-my-optimization-journey/results_details.htm).

If you want your solution to be listed and it passes 10K runs in less than 10 sec on 6 cores, please open a PR that makes your solution integrated into https://github.com/buybackoff/1brc-bench.

## Build & Run on Linux

To install .NET on Linux, follow [official instructions](https://learn.microsoft.com/en-us/dotnet/core/install/linux).

To build, run `./build.sh`. 

To run JIT version: `./jit.sh /path/to/measurements.txt`.

To run AOT version: `./aot.sh /path/to/measurements.txt`.