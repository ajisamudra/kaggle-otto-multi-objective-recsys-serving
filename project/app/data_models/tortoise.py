from tortoise import fields, models

# item features shape (556203, 10)
class Item_Feature(models.Model):
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


# item hour features shape (1506694, 6)
class Item_Hour_Feature(models.Model):
    aid = fields.IntField(null=True)
    hour = fields.IntField(null=True)
    itemXhour_click_count = fields.FloatField(null=True)
    itemXhour_click_to_cart_cvr = fields.FloatField(null=True)
    itemXhour_frac_click_all_click_count = fields.FloatField(null=True)
    itemXhour_frac_click_all_hour_click_count = fields.FloatField(null=True)


# item weekday features shape (1119686, 9)
class Item_Weekday_Feature(models.Model):
    aid = fields.IntField(null=True)
    weekday = fields.IntField(null=True)
    itemXweekday_all_events_count = fields.IntField(null=True)
    itemXweekday_cart_count = fields.IntField(null=True)
    itemXweekday_click_to_cart_cvr = fields.FloatField(null=True)
    itemXweekday_cart_to_order_cvr = fields.FloatField(null=True)
    itemXweekday_frac_click_all_click_count = fields.FloatField(null=True)
    itemXweekday_frac_cart_all_cart_count = fields.FloatField(null=True)
    itemXweekday_frac_cart_all_weekday_cart_count = fields.FloatField(null=True)


# item covisit clicks (8291708,3) (1/4 samples)
class Item_Covisit_Click_Weight(models.Model):
    aid_x = fields.IntField(null=True)
    aid_y = fields.IntField(null=True)
    wgt = fields.FloatField(null=True)


# item covisit buys (6385174,3) (1/4 samples)
class Item_Covisit_Buy_Weight(models.Model):
    aid_x = fields.IntField(null=True)
    aid_y = fields.IntField(null=True)
    wgt = fields.FloatField(null=True)


# item covisit buy2buy (11869997,3) (all samples)
class Item_Covisit_Buy2Buy_Weight(models.Model):
    aid_x = fields.IntField(null=True)
    aid_y = fields.IntField(null=True)
    wgt = fields.FloatField(null=True)
