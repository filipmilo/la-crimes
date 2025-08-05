#!/bin/bash

echo "> Starting Analytics Stack"

echo "> Creating docker network 'asvsp' (if not exists)"
docker network create asvsp 2>/dev/null || echo "Network 'asvsp' already exists"

echo ">> Starting up Metabase"
docker compose -f Metabase/docker-compose.yml up -d
echo ">> Waiting for Metabase to initialize..."
sleep 15

echo "> Analytics Stack started successfully!"
echo "> Metabase UI available at: http://localhost:3000"
echo "> Note: Metabase requires data from batch processing to be meaningful"
echo "> Recommended: Run batch processing first to populate data lake"