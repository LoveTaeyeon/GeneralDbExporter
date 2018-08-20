#!/usr/bin/python


def _adjust_heap(arr):
    i = len(arr) - 1
    while True:
        if i < len(arr) / 2:
            break
        temp_index = i
        while True:
            if temp_index == 0:
                break
            if temp_index % 2 == 0:
                parent_index = (temp_index - 2) / 2
            else:
                parent_index = (temp_index - 1) / 2
            if arr[parent_index] > arr[temp_index]:
                temp = arr[parent_index]
                arr[parent_index] = arr[temp_index]
                arr[temp_index] = temp
            temp_index = parent_index
        i -= 1


def add_number_to_heap(arr, number):
    arr[0] = arr[len(arr) - 1]
    arr[len(arr) - 1] = number
    _adjust_heap(arr)
