#!/bin/bash

dotnet build -c Release
dotnet publish -r linux-x64 -c Release