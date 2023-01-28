from tortoise import fields, models
from tortoise.contrib.pydantic import pydantic_model_creator

class Item_Features(models.Model):
    aid = fields.IntField(pk=True)
    item_all_events_count = fields.IntField(null=True)
    item_click_count = fields.IntField(null=True)
    item_cart_count = fields.IntField(null=True)
    item_order_count = fields.FloatField(null=True)
    item_avg_hour_click = fields.FloatField(null=True)
    item_avg_hour_cart = fields.FloatField(null=True)
    item_avg_hour_order = fields.FloatField(null=True)
    item_avg_weekday_click = fields.FloatField(null=True)
    item_avg_weekday_order = fields.FloatField(null=True)

class Item_Hour_Features(models.Model):
    aid_hour = fields.TextField(pk=True)
    aid = fields.IntField()
    hour = fields.IntField(null=True)
    itemXhour_click_count = fields.FloatField(null=True)
    itemXhour_click_to_cart_cvr = fields.FloatField(null=True)
    itemXhour_frac_click_all_click_count = fields.FloatField(null=True)
    itemXhour_frac_click_all_hour_click_count = fields.FloatField(null=True)

class Item_Weekday_Features(models.Model):
    aid_weekday = fields.TextField(pk=True)
    aid = fields.IntField()
    weekday = fields.IntField(null=True)
    itemXweekday_all_events_count = fields.IntField(null=True)
    itemXweekday_cart_count = fields.IntField(null=True)
    itemXweekday_click_to_cart_cvr = fields.FloatField(null=True)
    itemXweekday_cart_to_order_cvr = fields.FloatField(null=True)
    itemXweekday_frac_click_all_click_count = fields.FloatField(null=True)
    itemXweekday_frac_cart_all_cart_count = fields.FloatField(null=True)
    itemXweekday_frac_cart_all_weekday_cart_count = fields.FloatField(null=True)

# create Pydantic model for all Tortoise models

ItemFeaturesSchema = pydantic_model_creator(Item_Features)
ItemHourFeaturesSchema = pydantic_model_creator(Item_Hour_Features)
ItemWeekdayFeaturesSchema = pydantic_model_creator(Item_Weekday_Features)
