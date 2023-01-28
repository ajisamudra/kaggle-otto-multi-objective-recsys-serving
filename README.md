# kaggle-otto-multi-objective-recsys-serving

## Database

```bash
# create schema, at project dir
python app/db.py

# copy data from csv
# get into psql database web_dev
psql -U postgres -W
# switch to web_dev database
\c web_dev
# copy data to tables
\copy item_features FROM '/Users/ajisamudra/Documents/git/kaggle-otto-multi-objective-recsys-serving/project/db/data/test_item_feas.csv'DELIMITER ',' CSV HEADER
\copy item_hour_features FROM '/Users/ajisamudra/Documents/git/kaggle-otto-multi-objective-recsys-serving/project/db/data/test_item_hour_feas.csv'DELIMITER ',' CSV HEADER
\copy item_weekday_features FROM '/Users/ajisamudra/Documents/git/kaggle-otto-multi-objective-recsys-serving/project/db/data/test_item_weekday_feas.csv'DELIMITER ',' CSV HEADER
```
