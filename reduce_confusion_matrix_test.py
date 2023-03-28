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
        actual_reduced_matrix = pandas.read_csv(f'./examples/reduced_confusion_matrix_{top_mispredictions}.csv',index_col=0)
        assert(isinstance(output_reduced_matrix,pandas.DataFrame))
        assert(output_reduced_matrix.equals(actual_reduced_matrix))

def test_rcm_m_2():
    df_matrix = pandas.read_csv('./examples/confusion_matrix.csv',index_col=0)
    labels=list(df_matrix.index)
    confusion_matrix = df_matrix.to_numpy()
    top_misprediction = 2
    reduced_cm_df = reduce_confusion_matrix.reduce_confusion_matrix(confusion_matrix,labels,top_misprediction)
    assert(reduced_cm_df.loc["alarm_query","alarm_query"] == 0)
    assert(reduced_cm_df.loc["alarm_query","audio_volume_mute"] == 70)
    assert(reduced_cm_df.loc["alarm_query","Total"] == 70)
    assert(reduced_cm_df.loc["audio_volume_up","alarm_query"] == 60)
    assert(reduced_cm_df.loc["audio_volume_up","audio_volume_mute"] == 7)
    assert(reduced_cm_df.loc["audio_volume_up","Total"] == 67)
    assert(reduced_cm_df.loc["Total","alarm_query"] == 60)
    assert(reduced_cm_df.loc["Total","audio_volume_mute"] == 77)
    assert(reduced_cm_df.loc["Total","Total"] == 137)
    assert(reduced_cm_df.shape == (3,3))

    total_mispredictions = reduce_confusion_matrix.calc_total_mispredictions(confusion_matrix)
    assert(total_mispredictions == 360)

    actual_sorted_pair = [("alarm_query <-> audio_volume_mute",[70,19.44]),
                          ("audio_volume_up <-> alarm_query",[60,16.67]),
                          ("audio_volume_up <-> audio_volume_mute",[7,1.94]),
                          ("alarm_query <-> alarm_query",[0,0.0])]
    
    output_sorted_pair = reduce_confusion_matrix.find_top_intent_pair(reduced_cm_df,total_mispredictions)
    assert(actual_sorted_pair == output_sorted_pair)


def test_rcm_m_4():
    df_matrix = pandas.read_csv('./examples/confusion_matrix.csv',index_col=0)
    labels=list(df_matrix.index)
    confusion_matrix = df_matrix.to_numpy()
    top_misprediction = 4
    reduced_cm_df = reduce_confusion_matrix.reduce_confusion_matrix(confusion_matrix,labels,top_misprediction)
    assert(reduced_cm_df.shape == (5,5))
    assert(list(reduced_cm_df.index) == ["audio_volume_up", "alarm_query","alarm_set","audio_volume_mute","Total"])
    assert(list(reduced_cm_df.columns) == ["audio_volume_mute","alarm_query","audio_volume_up","audio_volume_down","Total"])
    assert(reduced_cm_df.loc["alarm_query","alarm_query"] == 0)
    assert(reduced_cm_df.loc["alarm_query","audio_volume_mute"] == 70)
    assert(reduced_cm_df.loc["audio_volume_up","alarm_query"] == 60)
    assert(reduced_cm_df.loc["audio_volume_up","audio_volume_mute"] == 7)
    assert(reduced_cm_df.loc["audio_volume_mute","audio_volume_down"] == 30)
    assert(reduced_cm_df.loc["Total","Total"] == 227)


    total_mispredictions = reduce_confusion_matrix.calc_total_mispredictions(confusion_matrix)
    assert(total_mispredictions == 360)

    actual_sorted_pair = [('alarm_query <-> audio_volume_mute', [70, 19.44]), 
                          ('audio_volume_up <-> alarm_query', [60, 16.67]), 
                          ('alarm_set <-> audio_volume_up', [40, 11.11]), 
                          ('audio_volume_mute <-> audio_volume_down', [30, 8.33]), 
                          ('audio_volume_up <-> audio_volume_mute', [11, 3.06]), 
                          ('audio_volume_up <-> audio_volume_down', [11, 3.06]), 
                          ('alarm_set <-> alarm_query', [5, 1.39]), 
                          ('audio_volume_up <-> audio_volume_up', [0, 0.0]), 
                          ('alarm_query <-> alarm_query', [0, 0.0]), 
                          ('alarm_query <-> audio_volume_down', [0, 0.0]), 
                          ('alarm_set <-> audio_volume_mute', [0, 0.0]), 
                          ('alarm_set <-> audio_volume_down', [0, 0.0]), 
                          ('audio_volume_mute <-> audio_volume_mute', [0, 0.0])]
    
    output_sorted_pair = reduce_confusion_matrix.find_top_intent_pair(reduced_cm_df,total_mispredictions)

    # case where two intersections having interchangable row and column names
    assert(actual_sorted_pair[4] == output_sorted_pair[4])

    # case where row and column name appears only once
    assert(actual_sorted_pair[6] == output_sorted_pair[6])
    