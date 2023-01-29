# kaggle-otto-multi-objective-recsys-serving

## Overview

## System Design

## API Spec

To see the details of the API specs, please refer to this [document]().

## Load test result

## Libraries

## Development

How to run the service

```bash
# build docker images
docker-compose build
# fire up the container
docker-compose up -d
# check logs for web service
docker-compose logs web

# get into shell
docker-compose exec web sh
docker-compose exec db sh


# setup database
docker-compose exec db psql -U postgres
# switch to web_dev
\c web_dev
```
