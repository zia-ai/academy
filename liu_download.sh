#!/usr/bin/env bash
cd data
rm liuetal.csv
echo ""
echo "Downloading data from https://github.com/xliuhw/NLU-Evaluation-Data:"
echo "Citation: Liu, X., Eshghi, A., Swietojanski, P. and Rieser, V., 2019. Benchmarking natural language understanding services for building conversational agents. arXiv preprint arXiv:1903.05566."
echo ""
wget https://github.com/xliuhw/NLU-Evaluation-Data/blob/master/AnnotatedData/NLU-Data-Home-Domain-Annotated-All.csv?raw=true -O liuetal.csv
cd ..