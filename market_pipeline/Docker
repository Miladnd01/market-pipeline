# ═══════════════════════════════════════════════════════════
# Stage 1: Build .NET Application
# ═══════════════════════════════════════════════════════════
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS dotnet-build
WORKDIR /app

# Copy and restore .NET project
COPY Maschin/*.csproj ./Maschin/
RUN dotnet restore ./Maschin/MaschinenDataein.csproj

# Build .NET app
COPY Maschin/ ./Maschin/
RUN dotnet publish ./Maschin/MaschinenDataein.csproj -c Release -o /app/dotnet-out

# ═══════════════════════════════════════════════════════════
# Stage 2: Final Runtime (Python + .NET Runtime)
# ═══════════════════════════════════════════════════════════
FROM mcr.microsoft.com/dotnet/aspnet:8.0

# Install Python and Supervisor
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy .NET application
COPY --from=dotnet-build /app/dotnet-out ./dotnet-app/

# Copy Python application
COPY market_pipeline/ ./python-app/

# Install Python dependencies
RUN pip3 install --no-cache-dir --break-system-packages -r ./python-app/requirements.txt

# Copy configuration files
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Environment variables
ENV ASPNETCORE_URLS=http://+:8080
ENV ASPNETCORE_ENVIRONMENT=Production

EXPOSE 8080

# Run both applications with Supervisor
CMD ["/app/start.sh"]
