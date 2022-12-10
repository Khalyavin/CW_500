import csv
import os


def select_sorted(sort_columns='[open]', order='asc', limit=10, filename='dump.csv'):
    print(f'select_sorted(sort_columns={sort_columns}, order={order}, limit={limit}, filename={filename}')


def get_by_date(date='2013-02-08', name='AAL', filename='dump.csv'):
    print(f'get_by_date(date={date}, name={name}, filename={filename}')


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
    path = '/home/vssl/PythonProject/cw_sp500/'
    data_file = 'all_stocks_5yr.csv'
    full_path = path + data_file

    start()

    print('Ого-го, работает!')


if __name__ == '__main__':
    main()
