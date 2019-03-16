#!/usr/bin/bash

for file in dataset/*.json; do
  echo "Cleaning for $file"
  filename=${file##*/}
  ipython -m src.cleaning $file dataset/cleaned/$filename
done
