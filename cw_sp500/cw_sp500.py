import csv
import os
import sys

sys.setrecursionlimit(1500)
path = '/home/vssl/PythonProject/cw_sp500/'
data_file = path + 'all_stocks_5yr.csv'
# full_path = path + data_file
data = []


def select_sorted(sort_columns='[open]', order='asc', limit=10, filename='dump.csv'):
    print(f'select_sorted(sort_columns={sort_columns}, order={order}, limit={limit}, filename={filename}')

def my_sort(data, field):
    ind = 0     # Сортировка по date
    if field == 'open': ind = 1
    elif field == 'close': ind = 2
    elif field == 'high': ind = 3
    elif field == 'low': ind = 4
    elif field == 'volume': ind = 5
    elif field == 'name': ind = 6

    if len(data) <= 1:
        return data
    else:
        pivot = data[len(data) // 2].split(',')[ind]
        min_data = []
        eq_data = []
        max_data = []
        for i in data:
            if i.split(',')[ind] < pivot:
                min_data.append(i)
            elif i.split(',')[ind] > pivot:
                max_data.append(i)
            else:
                eq_data.append(i)
        return (my_sort(min_data, ind) + eq_data + my_sort(max_data, ind))


def cach_date(dt, ticker, f_name):        # Был ли запрос ранее
    """Проверяет был ли такой запрос ранее - считать или искать кэш-файл"""
    cach_path = path + 'cach/'
    filename = str(dt) + str(ticker) + str(f_name) + '.csv'
    full_cach_filename = cach_path + filename
    print(full_cach_filename)
    if os.path.isfile(full_cach_filename):
        print(f'Данные по вашему запросу лежат в файле {filename}, путь {cach_path}')
        return True
    else:
        print('Ого-го,придётся поработать!')
        if not os.path.exists(cach_path):
            try:
                os.mkdir(cach_path)
            except OSError:
                print(f'Создать директорию {cach_path} не удалось')
            else:
                print(f'Директория {cach_path} успешно создана')

        # tmp_file = open(full_cach_filename, 'w+')
        # tmp_file.close()

        return False

def batch_date():
    """Нарезает файл данных на файлы малого размера для считывания
    файлика в оперативную память целиком для сортировки"""
    print('Шинкую файл данных на файлики в /tmp/batch_name_xx.csv')
    batch_num = 0
    header = []
    max_str_num = 100000
    str_cntr = 0
    batch_path = path + 'tmp/'
    batch_file = batch_path + f'batch_name_{str(batch_num) if len(str(batch_num)) == 2 else "0" + str(batch_num)}' + '.csv'

    with open(data_file, encoding='utf-8') as r_file:
        header = next(r_file)
        data = [header]
        for row in r_file:
            data.append(row)
            str_cntr += 1
            if str_cntr >= max_str_num:  # Наполнили очередной batch-файл
                with open(batch_file, 'w') as w_file:
                    w_file.writelines(data)
                batch_num += 1
                str_cntr = 0
                batch_file = batch_path + f'batch_name_{str(batch_num) if len(str(batch_num)) == 2 else "0" + str(batch_num)}' + '.csv'
                data = [header]
        if len(data) > 1:
            # batch_num += 1
            # batch_file = batch_path + f'batch_name_{str(batch_num) if len(str(batch_num)) == 2 else "0" + str(batch_num)}' + '.csv'
            with open(batch_file, 'w') as w_file:
                w_file.writelines(data)
    r_file.close()

def sort_date():
    """В директории /tmp лежат файлы batch_name_xx.csv, сортированные по тикеру.
    Считываю, сортирую по дате, записываю в batch_date_xx.csv"""
    print('Сортирую каждый файлик по дате')
    path_tmp = path + 'tmp/'
    files = os.listdir(path_tmp)
    cntr_date = 0    # Считаю количество нужных файлов
    cntr_name = 0
    header = []
    for file in files:
        if file.find('batch_date_'):
            cntr_date += 1
        if file.find('batch_name_'):
            cntr_name += 1

    if cntr_date == cntr_name:
        tmp = input('На диске уже есть отсортированные по дате файлики, сортируем по-новой? (y/n) :')
        if not (tmp == 'y' or tmp == 'Y'):
            return

    for i in range(cntr_name):
        batch_file_name = path_tmp + f'batch_name_{str(i) if len(str(i)) == 2 else "0" + str(i)}' + '.csv'
        batch_file_date = path_tmp + f'batch_date_{str(i) if len(str(i)) == 2 else "0" + str(i)}' + '.csv'

        print(f'Сортирую {batch_file_name}')
        with open(batch_file_name) as r_file:
            data = r_file.readlines()
        header.append(data[0])
        data = data[1::]
        data_sorted = my_sort(data, 'date')
        print(f'Отсортировано {batch_file_name}')

        with open(batch_file_date, 'w') as w_file:
            w_file.writelines(header)
            w_file.writelines(data_sorted)

        r_file.close()
        w_file.close()


def merge_date():
    print('Сливаю всё в файл /tmp/date_sorted.csv')

def batch_sort_date():
    print('Шинкую файл на сортированные файлики для поиска по запросу')

def search_date():
    print('Поиск по запросу и сброс результатов в файл для кэша')

def get_by_date(date='2013-02-08', name='AAL', filename='dump.csv'):
#    print(f'get_by_date(date={date}, name={name}, filename={filename}')
    if cach_date(date, name, filename):     # Определяю, нужно ли работать
        return
    batch_date()        # Шинкую файл данных на файлики в /tmp/banch_date_xx.csv
    input('Press any key')
    sort_date()         # Сортирую каждый файлик по дате
    merge_date()        # Сливаю всё в файл /tmp/date_sorted.csv
    batch_sort_date()   # Шинкую файл на сортированные файлики для поиска по запросу
    search_date()       # Поиск по запросу и сброс результатовв файл для кэша
    print('Завершил get_by_date.')

def start():
    print(' Сортировка по цене (1)  # select_sorted')
    print(' Сортировка по дате [2]  # get_by_date')
    tmp_sort_type = input('Ваш выбор: ')

    if tmp_sort_type == '1':
        print('Сортировать по:')
        print('цене [open]  (1)')
        print('цене [close] (2)')
        print('цене [high]  [3]')
        print('цене [low]   (4)')
        print('[volume]     (5)')
        tmp_col = input('Ваш выбор: ')

        if tmp_col == '1':
            sort_columns = '[open]'
        elif tmp_col == '2':
            sort_columns = '[close]'
        elif tmp_col == '4':
            sort_columns = '[low]'
        elif tmp_col == '5':
            sort_columns = '[volume]'
        else:
            sort_columns = '[high]'

        print()
        tmp_order = input('Порядок по убыванию [1] / возрастанию (2): ')
        if tmp_order == '2':
            order = 'asc'
        else:
            order = 'des'

        print()
        tmp_range = input('Ограничение выборки [10]: ')
        if tmp_range:
            limit = int(tmp_range)
        else:
            limit = 10

        print()
        tmp_filename = input('Название файла для сохранения результата [dump.csv]: ')
        if tmp_filename:
            filename = tmp_filename
        else:
            filename = 'dump.csv'

        select_sorted(sort_columns, order, limit, filename)
    else:
        tmp_date = input('Дата в формате yyyy-mm-dd [all]: ')
        if tmp_date:
            date = tmp_date
        else:
            date = 'all'

        print()
        tmp_ticker = input('Тикер [all]: ')
        if tmp_ticker:
            name = tmp_ticker
        else:
            name = 'all'

        print()
        tmp_filename = input('Файл сохранения [dump.csv] : ')
        if tmp_filename:
            filename = tmp_filename
        else:
            filename = 'dump.csv'

        get_by_date(date, name, filename)


def main():
    start()

    print('Ого-го, работает!')


if __name__ == '__main__':
    main()
