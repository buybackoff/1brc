#!/bin/bash

dotnet build -c Release 1brc/1brc.csproj
dotnet publish -r linux-x64  -f net8.0 -c Release 1brc/1brc.csproj