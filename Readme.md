# 1️⃣🐝🏎️ The One Billion Row Challenge

.NET implementation of https://github.com/gunnarmorling/1brc


## Results

**First attempt**

i5-12500/64GB RAM/Firecuda 530 (busy machine with 30+GB RAM used and YouTube music playing)

```
Processed in 00:00:10.6978618
Processed in 00:00:10.8473143
Processed in 00:00:10.9107262
Processed in 00:00:10.9733218
Processed in 00:00:10.5854176
```

**Some micro optimizations**

```
Processed in 00:00:09.7093471
```

Float parsing is ~57%, dictionary lookup is ~24%. Optimizing further is about those two things. We may use `csFastFloat` library and a specialized dictionary such as `DictionarySlim`. However the goal is to avoid dependencies even if they are pure .NET.

It's near-perfectly parallelizable though. On 8 cores it should be 33% faster than on 6 that I have. With 32GB RAM the file should be cached by an OS after the first read. The first read may be very slow in the cloud VM, but then the cache should eliminate the difference between drive speeds.