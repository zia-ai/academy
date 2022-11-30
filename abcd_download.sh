#!/usr/bin/env bash
echo 
echo Downloading the abcd data sample and data set from this paper:
echo Chen, D., Chen, H., Yang, Y., Lin, A. and Yu, Z., 2021. 
echo Action-based conversations dataset: A corpus for building more in-depth task-oriented dialogue systems. 
echo arXiv preprint arXiv:2104.00783.
echo 
cd data
rm abcd_sample.json*
wget https://github.com/asappresearch/abcd/blob/master/data/abcd_sample.json?raw=true -O abcd_sample.json
rm abcd_v1.1.json*
wget https://github.com/asappresearch/abcd/blob/master/data/abcd_v1.1.json.gz?raw=true -O abcd_v1.1.json.gz
gunzip abcd_v1.1.json.gz
cd ..