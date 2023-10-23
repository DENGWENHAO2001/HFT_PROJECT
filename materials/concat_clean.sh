# nohup python preprocess/concat_clean.py \
# --start_date 2023-03-30 --end_date 2023-05-15 \
# >preprocess/log/TUSD_clean.log 2>&1 &

nohup python preprocess/concat_clean.py \
    --symbols ETHUSDT --start_date 2022-05-01 --end_date 2022-06-15 \
    >preprocess/log/ETHUSDT_clean.log 2>&1 &

nohup python preprocess/concat_clean.py \
    --symbols GALAUSDT --start_date 2022-07-01 --end_date 2022-08-15 \
    >preprocess/log/GALAUSDT_clean.log 2>&1 &

nohup python preprocess/concat_clean.py \
    --symbols BTCUSDT --start_date 2022-09-01 --end_date 2022-10-15 \
    >preprocess/log/BTCUSDT_clean.log 2>&1 &
