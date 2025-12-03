# -*- coding: utf-8 -*-
from Pyro4 import expose

# Цей блок забезпечує сумісність між Python 2 (на сервері) і Python 3 (локально)
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
        # 1. Зчитування даних
        n, matrix = self.read_input()
        print("Input matrix loaded. Size: %dx%d" % (n, n))

        # 2. Отримуємо кількість воркерів
        k_workers = len(self.workers)
        print("Active workers: %d" % k_workers)

        # 3. Основний цикл Флойда-Варшалла (k - проміжна вершина)
        for k in xrange(n):
            # Рядок k (pivot_row) потрібен усім воркерам для обчислень
            pivot_row = matrix[k]
            
            mapped = []
            
            # Визначаємо розмір "шматка" для кожного воркера
            if k_workers > 0:
                # int() забезпечує цілочисельне ділення, необхідне для коректного індексування
                chunk_size = int((n + k_workers - 1) / k_workers)
            else:
                chunk_size = n

            # Розсилаємо задачі воркерам (Map)
            for i in xrange(k_workers):
                start_row = i * chunk_size
                end_row = min((i + 1) * chunk_size, n)
                
                if start_row < n:
                    # Вирізаємо частину матриці
                    chunk = matrix[start_row:end_row]
                    # Відправляємо задачу: (частина матриці, k-й рядок, номер ітерації)
                    mapped.append(self.workers[i].mymap(chunk, pivot_row, k))

            # Збираємо результати (Reduce)
            # Ми отримуємо оновлені шматки і склеюємо їх назад у матрицю
            matrix = self.myreduce(mapped)

        # 4. Запис результату
        self.write_output(matrix)
        print("Job Finished")

    @staticmethod
    @expose
    def mymap(chunk, pivot_row, k):
        # --- ЦЕЙ КОД ВИКОНУЄТЬСЯ НА ВОРКЕРІ ---
        updated_chunk = []
        
        for i in xrange(len(chunk)):
            row = chunk[i]
            d_ik = row[k] # Відстань від поточної вершини i до k
            
            new_row = []
            for j in xrange(len(row)):
                d_kj = pivot_row[j] # Відстань від k до j
                
                # Основна формула Флойда: D[i][j] = min(D[i][j], D[i][k] + D[k][j])
                val = min(row[j], d_ik + d_kj)
                new_row.append(val)
            
            updated_chunk.append(new_row)
            
        return updated_chunk

    @staticmethod
    @expose
    def myreduce(mapped):
        # --- ЦЕЙ КОД ВИКОНУЄТЬСЯ НА МАЙСТРІ ---
        output = []
        for chunk in mapped:
            # chunk.value отримує результат роботи воркера (масив оновлених рядків)
            output.extend(chunk.value) 
        return output

    def read_input(self):
        # Читання файлу
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
        # Запис файлу
        f = open(self.output_file_name, 'w')
        f.write(str(len(output)) + '\n')
        for row in output:
            row_str = ' '.join([str(x) for x in row])
            f.write(row_str + '\n')
        f.close()