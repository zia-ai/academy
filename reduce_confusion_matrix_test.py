#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80**************************************120
#
# Set of pytest reduce_confusion_matrix.py tests
#
# ***************************************************************************80**************************************120

import reduce_confusion_matrix
import pytest
import numpy
import pandas

def test_reduce_confusion_matrix():
    top_mispredictions_list = [0,1,4,81,117]
    df_matrix = pandas.read_csv('./examples/confusion_matrix.csv',index_col=0)
    labels=list(df_matrix.index)
    confusion_matrix = df_matrix.to_numpy()
    for top_misprediction in top_mispredictions_list:
        check_reduce_confusion_matrix(confusion_matrix,labels,top_misprediction)

def check_reduce_confusion_matrix(confusion_matrix: numpy.matrix, labels: list, top_mispredictions: int):
    if top_mispredictions == 0 or top_mispredictions > 81:
        with pytest.raises(Exception):
            output_reduced_matrix = reduce_confusion_matrix.reduce_confusion_matrix(confusion_matrix,labels,top_mispredictions)
    else:
        output_reduced_matrix = reduce_confusion_matrix.reduce_confusion_matrix(confusion_matrix,labels,top_mispredictions)
        output_reduced_matrix.to_csv(f"./examples/reduced_confusion_matrix_{top_mispredictions}.csv",sep=",",encoding="utf8")
        actual_reduced_matrix = pandas.read_csv(f'./examples/reduced_confusion_matrix_{top_mispredictions}.csv',index_col=0)
        assert(isinstance(output_reduced_matrix,pandas.DataFrame))
        assert(output_reduced_matrix.equals(actual_reduced_matrix))

def test_find_top_intent_pair():
    reduced_matrix = pandas.read_csv('./examples/reduced_confusion_matrix_4.csv',index_col=0)
    total_mispredictions = 2583
    output_sorted_pair = reduce_confusion_matrix.find_top_intent_pair(reduced_matrix,total_mispredictions)
    print(output_sorted_pair)
    actual_sorted_pair = [('alarm_query <-> audio_volume_mute', [70, 2.71]), ('audio_volume_up <-> alarm_query', [60, 2.32]), 
                          ('alarm_set <-> audio_volume_up', [40, 1.55]), ('audio_volume_mute <-> audio_volume_down', [30, 1.16]), 
                          ('audio_volume_up <-> audio_volume_mute', [11, 0.43]), ('audio_volume_up <-> audio_volume_down', [11, 0.43]), 
                          ('alarm_set <-> alarm_query', [5, 0.19]), ('audio_volume_up <-> audio_volume_up', [0, 0.0]), 
                          ('alarm_query <-> alarm_query', [0, 0.0]), ('alarm_query <-> audio_volume_down', [0, 0.0]), 
                          ('alarm_set <-> audio_volume_mute', [0, 0.0]), ('alarm_set <-> audio_volume_down', [0, 0.0]), 
                          ('audio_volume_mute <-> audio_volume_mute', [0, 0.0])]
    assert(output_sorted_pair == actual_sorted_pair)