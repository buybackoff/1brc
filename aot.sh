#!/bin/bash

hyperfine --warmup 1 --runs 5 "./1brc/bin/Release/net8.0/linux-x64/publish/1brc $1 "