nohup python preprocess/merge_new.py \
    --data_path preprocess/create_features/BTCTUSD/2023-03-30-2023-05-15/beatrate_0.0001 \
    >preprocess/log/BTCTUSD_merge.log 2>&1 &

nohup python preprocess/merge_new.py \
    --data_path preprocess/create_features/BTCUSDT/2022-09-01-2022-10-15/beatrate_0.0001 \
    >preprocess/log/BTCUSDT_merge.log 2>&1 &


nohup python preprocess/merge_new.py \
    --data_path preprocess/create_features/ETHUSDT/2022-05-01-2022-06-15/beatrate_0.0001 \
    >preprocess/log/ETHUSDT_merge.log 2>&1 &

nohup python preprocess/merge_new.py \
    --data_path preprocess/create_features/GALAUSDT/2022-07-01-2022-08-15/beatrate_0.0001 \
    >preprocess/log/GALAUSDT_merge.log 2>&1 &