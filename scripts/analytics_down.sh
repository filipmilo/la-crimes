#!/bin/bash

echo "> Bringing down Analytics Stack"

read -p "> Delete volumes? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
  echo ">> Shutting down Metabase"
  docker compose -f Metabase/docker-compose.yml down -v
else
  echo ">> Shutting down Metabase"
  docker compose -f Metabase/docker-compose.yml down
fi

echo "> Analytics Stack stopped"
echo "> Note: Docker network 'asvsp' preserved for other services"