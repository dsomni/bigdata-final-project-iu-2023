#!/bin/bash


rm -f ./data/*.zip
rm -f ./data/*.json
rm -f ./data/*.csv

wget --load-cookies /tmp/cookies.txt "https://docs.google.com/uc?export=download&confirm=$(wget --quiet --save-cookies /tmp/cookies.txt --keep-session-cookies --no-check-certificate 'https://docs.google.com/uc?export=download&id=FILEID' -O- | sed -rn 's/.*confirm=([0-9A-Za-z_]+).*/\1\n/p')&id=1u92tj62G-OWoD6LWd3Tj0C3iMilJyBim" -O ./data/archive.zip && rm -rf /tmp/cookies.txt

unzip -o ./data/archive.zip -d ./data

rm -f ./output/*.csv
rm -f ./output/*.json
rm -f ./output/*.log

python ./scripts/preprocess.py
