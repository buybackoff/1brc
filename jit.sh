#!/bin/bash

hyperfine --warmup 1 --runs 5 "./1brc/bin/Release/net8.0/1brc $1 "