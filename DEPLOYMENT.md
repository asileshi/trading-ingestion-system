# Deployment (EC2)

## Start / update
git pull
docker compose up -d --build

## Status
docker compose ps

## Logs
docker compose logs -f --tail=200 api
docker compose logs -f --tail=200 worker

## Stop
docker compose down
