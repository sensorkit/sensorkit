FROM alpine:3.24 AS base

ARG package_url="https://github.com/ASCOMInitiative/ASCOM.Alpaca.Simulators/archive/refs/tags/v0.4.0.tar.gz"
ARG tar_opts="-z"

RUN apk add --no-cache wget

WORKDIR /src

RUN wget "${package_url}" -O- | tar -x ${tar_opts}

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
COPY --from=base /src/* /app

WORKDIR /app 
RUN dotnet restore

WORKDIR /app/ASCOM.Alpaca.Simulators
RUN dotnet publish -c Release -o out

# Build runtime image
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app

COPY --from=build /app/ASCOM.Alpaca.Simulators/out ./
