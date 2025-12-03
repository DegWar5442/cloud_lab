# -*- coding: utf-8 -*-
from Pyro4 import expose
import time

# ХАК ДЛЯ СУМІСНОСТІ: Використання range або xrange
try:
    xrange
except NameError:
    xrange = range

class Solver:
    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.workers = workers

    def solve(self):
        start_global_time = time.time()
        print("Job Started")
        
        # 1. Зчитування даних
        n, matrix = self.read_input()
        
        k_workers = len(self.workers)
        print("Input matrix loaded. Size: %dx%d" % (n, n))
        print("Active workers: %d" % k_workers)

        # 2. Основний цикл Флойда-Варшалла (k - проміжна вершина)
        for k in xrange(n):
            pivot_row = matrix[k]
            mapped = []
            
            if k_workers == 0:
                # Якщо воркерів немає, Майстер виконує все самостійно (послідовно)
                for i in xrange(n):
                    for j in xrange(n):
                        matrix[i][j] = min(matrix[i][j], matrix[i][k] + matrix[k][j])
                continue # Переходимо до наступної ітерації k

            # Розрахунок розміру "шматка"
            chunk_size = int((n + k_workers - 1) / k_workers)

            # Розсилаємо задачі воркерам (Map)
            for i in xrange(k_workers):
                start_row = i * chunk_size
                end_row = min((i + 1) * chunk_size, n)
                
                if start_row < n:
                    # Оптимізація 1: Вирізаємо частину матриці
                    chunk = matrix[start_row:end_row]
                    
                    # Відправляємо задачу
                    mapped.append(self.workers[i].mymap(chunk, pivot_row, k))

            # Збираємо результати (Reduce)
            new_matrix = self.myreduce(mapped)
            
            # Оптимізація 2: Замість склеювання всієї матриці, 
            # оновлюємо лише ті рядки, які були оброблені.
            # Оскільки розподіл рядків є статичним, ми можемо оновлювати матрицю послідовно
            
            current_row = 0
            for chunk in new_matrix:
                for row_data in chunk:
                    matrix[current_row] = row_data
                    current_row += 1

        # 4. Запис результату
        self.write_output(matrix)
        end_global_time = time.time()
        print("Job Finished in %f seconds" % (end_global_time - start_global_time))

    @staticmethod
    @expose
    def mymap(chunk, pivot_row, k):
        # --- ЦЕЙ КОД ВИКОНУЄТЬСЯ НА ВОРКЕРІ ---
        updated_chunk = []
        
        for i in xrange(len(chunk)):
            row = chunk[i]
            d_ik = row[k] 
            
            new_row = []
            for j in xrange(len(row)):
                d_kj = pivot_row[j] 
                val = min(row[j], d_ik + d_kj)
                new_row.append(val)
            
            updated_chunk.append(new_row)
            
        return updated_chunk

    @staticmethod
    @expose
    def myreduce(mapped):
        # --- ЦЕЙ КОД ВИКОНУЄТЬСЯ НА МАЙСТРІ ---
        # Повертаємо список оновлених шматків, щоб уникнути зайвого extend()
        output = []
        for chunk in mapped:
            output.append(chunk.value) 
        return output

    def read_input(self):
        # Читання файлу (використовуємо більш надійний метод)
        f = open(self.input_file_name, 'r')
        lines = f.readlines()
        f.close()
        
        n = int(lines[0].strip())
        matrix = []
        for line in lines[1:]:
            if line.strip():
                parts = line.split()
                row = [int(x) for x in parts]
                matrix.append(row)
        return n, matrix

    def write_output(self, output):
        # Запис файлу (з оновленими print statement для часу)
        f = open(self.output_file_name, 'w')
        f.write(str(len(output)) + '\n')
        for row in output:
            row_str = ' '.join([str(x) for x in row])
            f.write(row_str + '\n')
        f.close()