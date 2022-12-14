import csv
import os
import sys
import shutil

sys.setrecursionlimit(1500)
path = '/home/vssl/PythonProject/cw_sp500/'
data_file = path + 'all_stocks_5yr.csv'
# full_path = path + data_file
data = []

def sort_by_date(str): return str[0]
def sort_by_high(str): return str[1]
def sort_by_open(str): return str[2]
def sort_by_close(str): return str[3]
def sort_by_low(str): return str[4]
def sort_by_volume(str): return int(str[5])

def select_sorted(sort_columns='[open]', order='asc', limit=10, ticker='AAL', filename='dump.csv'):
    print(f'select_sorted(sort_columns={sort_columns}, order={order}, limit={limit}, ticker={ticker}, filename={filename}')

    header = []

    with open(data_file, 'r') as file:
        file_reader = csv.reader(file)
        for row in file_reader:
            if row[0] == 'date':
                header.append(row)
            else:
                if row[6] == ticker:
                    data.append(row)

    if sort_columns == 0: data.sort(key=sort_by_date)
    if sort_columns == 1: data.sort(key=sort_by_high)
    if sort_columns == 2: data.sort(key=sort_by_open)
    if sort_columns == 3: data.sort(key=sort_by_close)
    if sort_columns == 4: data.sort(key=sort_by_low)
    if sort_columns == 5: data.sort(key=sort_by_volume)

    print(header)
    if order == 'acs':
        for i in range(limit):
            print(data[i])
    else:
        for i in range(len(data) - 1, len(data) - limit - 1, -1):
            print(data[i])


def my_sort(data, field):
    ind = 0  # Сортировка по date
    if field == 'open':
        ind = 1
    elif field == 'close':
        ind = 2
    elif field == 'high':
        ind = 3
    elif field == 'low':
        ind = 4
    elif field == 'volume':
        ind = 5
    elif field == 'name':
        ind = 6

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


def cach_date(dt, ticker, f_name):  # Был ли запрос ранее
    """Проверяет был ли такой запрос ранее - считать или искать кэш-файл"""
    cach_path = path + 'cach/'
    filename = str(dt) + str(ticker) + str(f_name)
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

        return False


def batch_date():
    """Нарезает файл данных на файлы малого размера для считывания
    файлика в оперативную память целиком для сортировки"""
    print('Шинкую файл данных на файлики в /tmp/batch_name_xx.csv')

    batch_path = path + 'tmp/'

    if not os.path.exists(batch_path):
        try:
            os.mkdir(batch_path)
        except OSError:
            print(f'Создать директорию {batch_path} не удалось')
        else:
            print(f'Директория {batch_path} успешно создана')

    batch_num = 0
    header = []
    max_str_num = 100000
    str_cntr = 0
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
    cntr_date = 0  # Считаю количество нужных файлов
    cntr_name = 0
    header = []
    for file in files:
        if file.find('batch_date_') != -1:
            cntr_date += 1
        if file.find('batch_name_') != -1:
            cntr_name += 1

    if cntr_date == cntr_name:
        tmp = input('На диске уже есть отсортированные по дате файлики, сортируем по-новой? (y/[n]) :')
        if not (tmp == 'y' or tmp == 'Y'):
            return

    for i in range(cntr_name):
        batch_file_name = path_tmp + f'batch_name_{str(i) if len(str(i)) == 2 else "0" + str(i)}' + '.csv'
        batch_file_date = path_tmp + f'batch_date_{str(i) if len(str(i)) == 2 else "0" + str(i)}' + '.csv'

        print(f'Сортирую {batch_file_name}')
        with open(batch_file_name) as r_file:
            data = r_file.readlines()

        if len(header) == 0: header.append(data[0])

        data = data[1::]
        data_sorted = my_sort(data, 'date')
        print(f'Отсортировано {batch_file_name}')

        with open(batch_file_date, 'w') as w_file:
            w_file.writelines(header)
            w_file.writelines(data_sorted)

        r_file.close()
        w_file.close()


def merge_tail(pointer, write_file):
    for row in pointer:
        write_file.writerow(row)


def merge_date():
    """Из отсортированных по дате файлов /tmp/batch_date_xx.csv
    собираю большой отсортированный /tmp/date_sorted.csv"""
    print('Сливаю всё в файл /tmp/date_sorted.csv')
    path_tmp = path + 'tmp/'
    files = os.listdir(path_tmp)
    cntr_date = 0  # Считаю количество нужных файлов
    cntr_big_sorted_file = 0
    header = []
    for file in files:
        if file.find('batch_date_') != -1:
            cntr_date += 1
        if file.find('date_sorted.csv') != -1:
            cntr_big_sorted_file += 1

    if cntr_date > 1 and cntr_big_sorted_file > 0:
        tmp = input('На диске уже есть отсортированный и слитый файл, сливать файлы по-новой? (y/[n]) :')
        if not (tmp == 'y' or tmp == 'Y'):
            return

    for i in range(cntr_date):
        file_i = path_tmp + f'{str(i) if len(str(i)) == 2 else "0" + str(i)}' + '.csv'
        batch_file_date = path_tmp + f'batch_date_{str(i) if len(str(i)) == 2 else "0" + str(i)}' + '.csv'

        if i == 0:
            shutil.copy(batch_file_date, file_i)  # Файл 00.csv тупо копирую из batch_date_00.csv
        else:
            prev_file = open(path_tmp + f'{str(i - 1) if len(str(i - 1)) == 2 else "0" + str(i - 1)}' + '.csv')
            prev_reader = csv.reader(prev_file)  # Большой слитый отсортированный файл
            header = next(prev_reader)

            date_file = open(path_tmp + f'batch_date_{str(i) if len(str(i)) == 2 else "0" + str(i)}' + '.csv')
            date_reader = csv.reader(date_file)
            tmp = next(date_reader)

            res_file = open(path_tmp + f'{str(i) if len(str(i)) == 2 else "0" + str(i)}' + '.csv', 'w')
            res_writer = csv.writer(res_file)  # Файл, куда сливаю отсортированные данные
            res_writer.writerow(header)

            # Считал первые строки файлов - источников
            row_prev = next(prev_reader)
            row_date = next(date_reader)

            while True:
                if row_prev[0] < row_date[0]:
                    res_writer.writerow(row_prev)
                    try:
                        row_prev = next(prev_reader)
                    except:  # prev_file закончился, остатки date_file сливаю в res_file
                        merge_tail(date_reader, res_writer)
                        break
                elif row_prev[0] > row_date[0]:
                    res_writer.writerow(row_date)
                    try:
                        row_date = next(date_reader)
                    except:  # date_file закончился, остатки prev_file сливаю в res_file
                        merge_tail(prev_reader, res_writer)
                        break
                else:  # Поля date равны, смотрю поле name
                    if row_prev[6] < row_date[6]:
                        res_writer.writerow(row_prev)
                        try:
                            row_prev = next(prev_reader)
                        except:  # prev_file закончился, остатки date_file сливаю в res_file
                            merge_tail(date_reader, res_writer)
                            break
                    else:
                        res_writer.writerow(row_date)
                        try:
                            row_date = next(date_reader)
                        except:  # date_file закончился, остатки prev_file сливаю в res_file
                            merge_tail(prev_reader, res_writer)
                            break

            prev_file.close()
            date_file.close()
            res_file.close()

    file_source = path_tmp + f'{str(cntr_date - 1) if len(str(cntr_date - 1)) == 2 else "0" + str(cntr_date - 1)}' + '.csv'
    file_dest = path_tmp + 'date_sorted.csv'
    shutil.copy(file_source, file_dest)


def batch_sort_date():
    """Нарезает файл данных на файлы малого размера для считывания
    файлика в оперативную память целиком для сортировки"""
    print('Шинкую отсортированный файл данных на файлики в /tmp/batch_sort_xx.csv')
    batch_num = 0
    header = []
    max_str_num = 100000
    str_cntr = 0
    batch_path = path + 'tmp/'
    batch_file = batch_path + f'batch_sort_{str(batch_num) if len(str(batch_num)) == 2 else "0" + str(batch_num)}' + '.csv'
    sort_file = batch_path + 'date_sorted.csv'

    with open(sort_file, encoding='utf-8') as r_file:
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
                batch_file = batch_path + f'batch_sort_{str(batch_num) if len(str(batch_num)) == 2 else "0" + str(batch_num)}' + '.csv'
                data = [header]
        if len(data) > 1:
            with open(batch_file, 'w') as w_file:
                w_file.writelines(data)
    r_file.close()


def seek_data(f_name, date, ticker, cach_file):
    path_cach = path + 'cach/'
    fp = open(f_name)
    data_for_search = fp.readlines()
    fp.close()

    cach_file = path_cach + str(date) + str(ticker) + str(cach_file)
    fp = open(cach_file, 'w')
    fp.writelines(data_for_search[0])

    if date == 'all':  # Все данные по одному тикеру
        for i in range(1, len(data_for_search)):
            if data_for_search[i].split(',')[6].replace('\n', '') == ticker:
                fp.writelines(data_for_search[i])
            else:
                continue
    elif ticker == 'all':  # Все тикеры за одну дату
        for i in range(1, len(data_for_search)):
            if data_for_search[i].split(',')[0] == date:
                fp.writelines(data_for_search[i])
            else:
                continue
    else:
        for i in range(1, len(data_for_search)):
            if data_for_search[i].split(',')[0] == date and data_for_search[i].split(',')[6].replace('\n', '') == ticker:
                fp.writelines(data_for_search[i])
                break

    fp.close()

    print(f'Запрашиваемые данные в файле {cach_file}, путь {path_cach}')


def search_date(date, ticker, f_name):
    print('Поиск по запросу и сброс результатов в файл для кэша')

    path_tmp = path + 'tmp/'
    files = os.listdir(path_tmp)
    cntr_sort = 0  # Считаю количество нужных файлов
    cntr_name = 0

    for file in files:
        if file.find('batch_date_') != -1:
            cntr_sort += 1
        if file.find('batch_name_') != -1:
            cntr_name += 1

    if cntr_sort != cntr_name:
        print('Необходимо пересчитать выходные данные.')
        return
    else:
        if cntr_sort > 0:
            dict_name = {}
            dict_sort = {}
            for i in range(cntr_sort):
                tmp_f_name = path_tmp + f'batch_sort_{str(i) if len(str(i)) == 2 else "0" + str(i)}' + '.csv'
                sort_file = open(tmp_f_name)
                tmp = sort_file.readlines()
                dict_sort.update({tmp_f_name: [tmp[1].split(',')[0], tmp[-1].split(',')[0]]})
                sort_file.close()

                tmp_f_name = path_tmp + f'batch_name_{str(i) if len(str(i)) == 2 else "0" + str(i)}' + '.csv'
                name_file = open(tmp_f_name)
                tmp = name_file.readlines()
                dict_name.update(
                    {tmp_f_name: [tmp[1].split(',')[6].replace('\n', ''), tmp[-1].split(',')[6].replace('\n', '')]})
                name_file.close()

    if date == 'all' and ticker == 'all':
        print(f'Данные по вашему запросу лежат в файле "all_stocks_5yr.csv", путь {path}')
    elif date == 'all' and ticker != 'all':
        for k, v in dict_name.items():
            if v[0] <= ticker <= v[1]:
                seek_data(k, date, ticker, f_name)
                return
    elif date != 'all' and ticker == 'all':
        for k, v in dict_sort.items():
            if v[0] <= date <= v[1]:
                seek_data(k, date, ticker, f_name)
                return
    else:
        for k, v in dict_sort.items():
            if v[0] <= date <= v[1]:
                seek_data(k, date, ticker, f_name)
                return


def get_by_date(date='2013-02-08', name='AAL', filename='dump.csv'):
    if cach_date(date, name, filename):  # Определяю, нужно ли работать
        return
    batch_date()  # Шинкую файл данных на файлики в /tmp/banch_date_xx.csv
    sort_date()  # Сортирую каждый файлик по дате
    merge_date()  # Сливаю всё в файл /tmp/date_sorted.csv
    batch_sort_date()  # Шинкую файл на сортированные файлики для поиска по запросу
    search_date(date, name, filename)  # Поиск по запросу и сброс результатовв файл для кэша
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
            order = 'des'
        else:
            order = 'asc'

        print()
        tmp_range = input('Ограничение выборки [10]: ')
        if tmp_range:
            limit = int(tmp_range)
        else:
            limit = 10

        print()
        tmp_ticker = input('Тикер для выборки [AAL]: ')
        if tmp_ticker:
            ticker = tmp_ticker
        else:
            ticker  = 'AAL'

        print()
        tmp_filename = input('Название файла для сохранения результата [dump.csv]: ')
        if tmp_filename:
            filename = tmp_filename
        else:
            filename = 'dump.csv'

        select_sorted(sort_columns, order, limit, ticker, filename)
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
