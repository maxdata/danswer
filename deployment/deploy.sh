# build from pull
docker compose -f docker-compose.dev.yml -p danswer-stack up -d --pull always --force-recreate

# to build from source
docker compose -f docker-compose.dev.yml -p danswer-stack up -d --build --force-recreate 

# To shut down the deployment, run:
# To stop the containers: 
docker compose -f docker-compose.dev.yml -p danswer-stack stop
# To delete the containers: 
docker compose -f docker-compose.dev.yml -p danswer-stack down
# To completely remove Danswer run:
# WARNING, this will also erase your indexed data and users
docker compose -f docker-compose.dev.yml -p danswer-stack down -v
