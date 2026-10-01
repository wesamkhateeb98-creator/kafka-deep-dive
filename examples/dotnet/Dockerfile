# Builds ANY example as a container image.
#   docker build -f examples/dotnet/Dockerfile --build-arg PROJECT=13-EventDriven -t kafka-example-13 examples/dotnet
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG PROJECT=13-EventDriven
WORKDIR /src
COPY Directory.Build.props ./
COPY Common/ Common/
COPY ${PROJECT}/ ${PROJECT}/
RUN dotnet publish ${PROJECT}/${PROJECT}.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
ARG PROJECT=13-EventDriven
ENV APP_DLL=${PROJECT}.dll
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["sh", "-c", "exec dotnet $APP_DLL \"$@\"", "--"]
