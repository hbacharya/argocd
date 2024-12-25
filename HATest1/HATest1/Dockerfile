# Use the official ASP.NET runtime as a base image
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 8080

# Use the official .NET SDK image to build the application
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["HATest1.csproj", "."]
RUN dotnet restore "HATest1.csproj"
COPY . .
WORKDIR "/src"
RUN dotnet build "HATest1.csproj" -c Release -o /app/build

# Publish the application
FROM build AS publish
RUN dotnet publish "HATest1.csproj" -c Release -o /app/publish /p:UseAppHost=false

# Use the base image to run the application
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "HATest1.dll"]
