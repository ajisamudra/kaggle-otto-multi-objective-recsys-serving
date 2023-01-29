# pull official base image
FROM postgres:14-alpine

WORKDIR $HOME/serving_db

# run create.sql on init
ADD create.sql /docker-entrypoint-initdb.d
