from tortoise import fields, models
from tortoise.contrib.pydantic import pydantic_model_creator

class Item_Features(models.Model):
    aid = fields.IntField()
    item_all_events_count = fields.IntField()
    item_click_count = fields.IntField()
    item_cart_count = fields.IntField()
    item_order_count = fields.IntField()
    item_avg_hour_click = fields.FloatField()
    item_avg_hour_cart = fields.FloatField()
    item_avg_hour_order = fields.FloatField()
    item_avg_weekday_click = fields.FloatField()
    item_avg_weekday_order = fields.FloatField()

class Item_Hour_Features(models.Model):
    aid = fields.IntField()
    hour = fields.IntField()
    itemXhour_click_count = fields.IntField()
    itemXhour_click_to_cart_cvr = fields.FloatField()
    itemXhour_frac_click_all_click_count = fields.FloatField()
    itemXhour_frac_click_all_hour_click_count = fields.FloatField()

class Item_Weekday_Features(models.Model):
    aid = fields.IntField()
    weekday = fields.IntField()
    itemXweekday_all_events_count = fields.IntField()
    itemXweekday_cart_count = fields.IntField()
    itemXweekday_click_to_cart_cvr = fields.FloatField()
    itemXweekday_cart_to_order_cvr = fields.FloatField()
    itemXweekday_frac_click_all_click_count = fields.FloatField()
    itemXweekday_frac_cart_all_cart_count = fields.FloatField()
    itemXweekday_frac_cart_all_weekday_cart_count = fields.FloatField()

# create Pydantic model for all Tortoise models

ItemFeaturesSchema = pydantic_model_creator(Item_Features)
ItemHourFeaturesSchema = pydantic_model_creator(Item_Hour_Features)
ItemWeekdayFeaturesSchema = pydantic_model_creator(Item_Weekday_Features)
