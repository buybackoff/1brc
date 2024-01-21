# 1️⃣🐝🏎️ The One Billion Row Challenge

.NET implementation of https://github.com/gunnarmorling/1brc

See my detailed blog post about my 1BRC journey here: https://hotforknowledge.com/2024/01/13/1brc-in-dotnet-among-fastest-on-linux-my-optimization-journey/

## Results

See [aggregated results](https://hotforknowledge.com/2024/01/13/1brc-in-dotnet-among-fastest-on-linux-my-optimization-journey/#results) in the blog post. Don't miss [a link to details avg/min/max/sigma results](https://hotforknowledge.com/2024/01/13/1brc-in-dotnet-among-fastest-on-linux-my-optimization-journey/results_details.htm).

If you want your solution to be listed and it passes 10K runs in less than 10 sec on 6 cores, please open an issue with a link to your code.

## Build & Run on Linux

To install .NET on Linux, follow [official instructions](https://learn.microsoft.com/en-us/dotnet/core/install/linux).

To build, run `./build.sh`. 

To run JIT version: `./jit.sh /path/to/measurements.txt`.

To run AOT version: `./aot.sh /path/to/measurements.txt`.