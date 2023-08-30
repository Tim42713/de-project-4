Список полей и их сточников используемых для построения витрины:
* courier_id — dds.dm_courier;
* courier_name — dds.dm_courier;
- settlement_year — dds.dm_timestamp;
    * settlement_month — dds.dm_timestamp;
    * orders_count — dm_deliveries;
    * orders_total_sum — dm_orders;
    * rate_avg — dm_deliveries;
    * order_processing_fee — orders_total_sum * 0.25;
    * courier_order_sum — orders_total_sum, rate_avg;
    * courier_tips_sum — dm_deliveries(tip_sum);
    * courier_reward_sum — courier_order_sum + courier_tips_sum * 0.95.

Список таблиц в слое DDS, из которых мы возьмём поля для витрины:
    * dm_timestamps;
    * dm_orders;
    * dm_deliveries;
    * dm_couriers.

 На основе списка таблиц в DDS составим список сущностей и полей, которые необходимо загрузить из API
    * dm_timestamp и dm_orders были получены ранее в спринте;
    * dm_deliveries и dm_courier получим данные по API используя методы /couriers и /deliveries.

* Так же использовал /restaurants согласно заданию, хотя данные можно получить из системы заказов.
