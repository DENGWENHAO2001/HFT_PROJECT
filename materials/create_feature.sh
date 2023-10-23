nohup python preprocess/create_feature.py \
--data_path preprocess/concat_clean/BTCTUSD/2023-03-30-2023-05-15 \
>preprocess/log/BTCTUSD_create.log 2>&1 &


nohup python preprocess/create_feature.py \
--data_path preprocess/concat_clean/BTCUSDT/2022-09-01-2022-10-15 \
>preprocess/log/BTCUSDT_create.log 2>&1 &


nohup python preprocess/create_feature.py \
--data_path preprocess/concat_clean/ETHUSDT/2022-05-01-2022-06-15 \
>preprocess/log/ETHUSDT_create.log 2>&1 &


nohup python preprocess/create_feature.py \
--data_path preprocess/concat_clean/GALAUSDT/2022-07-01-2022-08-15 \
>preprocess/log/GALAUSDT_create.log 2>&1 &