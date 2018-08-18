# Quick Sorted
arr = [3, 1, 2, 7, 9, 4, 5, 10, 8, 6]


def quick_sort(arr, begin_number, end_number):
    i = begin_number + 1
    j = end_number - 1
    key_num = arr[begin_number]
    smaller_is_find = False
    bigger_is_find = False
    temp_value = None
    while True:
        if i >= end_number or j <= begin_number:
            break
        if arr[i] < key_num:
            i += 1
        else:
            bigger_is_find = True
        if arr[j] > key_num:
            j -= 1
        else:
            smaller_is_find = True
        if bigger_is_find and smaller_is_find:
            temp = arr[i]
            arr[i] = arr[j]
            arr[j] = temp
            smaller_is_find = False
            bigger_is_find = False
        if i >= j:
            change_number = None
            if arr[i] <= arr[begin_number]:
                change_number = i
            else:
                change_number = i - 1
            temp_value = arr[begin_number]
            arr[begin_number] = arr[change_number]
            arr[change_number] = temp_value
            if i - begin_number > 1:
                quick_sort(arr, begin_number, i)
            if end_number - i > 1:
                quick_sort(arr, i, end_number)
            break


quick_sort(arr, 0, len(arr))
print(arr)
