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
# item features
\copy item_feature FROM '/Users/ajisamudra/Documents/git/kaggle-otto-multi-objective-recsys-serving/project/db/data/test_item_feas.csv'DELIMITER ',' CSV HEADER

# item-hour features
\copy item_hour_feature FROM '/Users/ajisamudra/Documents/git/kaggle-otto-multi-objective-recsys-serving/project/db/data/test_item_hour_feas.csv'DELIMITER ',' CSV HEADER

# item-weekday features
\copy item_weekday_feature FROM '/Users/ajisamudra/Documents/git/kaggle-otto-multi-objective-recsys-serving/project/db/data/test_item_weekday_feas.csv'DELIMITER ',' CSV HEADER

# item-covisit-clicks weights
\copy item_covisit_click_weight FROM '/Users/ajisamudra/Documents/git/kaggle-otto-multi-objective-recsys-serving/project/db/data/top20_covisitation_clicks.csv'DELIMITER ',' CSV HEADER

# item-covisit-buys weights
\copy item_covisit_buy_weight FROM '/Users/ajisamudra/Documents/git/kaggle-otto-multi-objective-recsys-serving/project/db/data/top15_covisitation_buys.csv'DELIMITER ',' CSV HEADER

# item-covisit-buy2buy2 weights
\copy item_covisit_buy2buy_weight FROM '/Users/ajisamudra/Documents/git/kaggle-otto-multi-objective-recsys-serving/project/db/data/top15_covisitation_buy2buy.csv'DELIMITER ',' CSV HEADER

```
