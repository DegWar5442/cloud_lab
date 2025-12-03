import random

# Константа для нескінченності та розміру
INF = 999
N = 200  # Розмір матриці
DENSITY = 0.4  # Щільність графа (40% ребер присутні)

def generate_floyd_input(n, filename):
    """Генерує випадкову матрицю суміжності N x N."""
    matrix = []
    
    for i in range(n):
        row = []
        for j in range(n):
            if i == j:
                row.append(0)  # Відстань до себе = 0
            elif random.random() < DENSITY:
                # Випадкова вага ребра (від 1 до 100)
                row.append(random.randint(1, 100))
            else:
                # Відсутність ребра (нескінченність)
                row.append(INF)
        matrix.append(row)

    # Запис у файл у форматі PARCS
    with open(filename, 'w') as f:
        f.write(str(n) + '\n')
        for row in matrix:
            f.write(' '.join(map(str, row)) + '\n')
    
    print(f"Файл '{filename}' (розмір {n}x{n}) успішно згенеровано.")

if __name__ == '__main__':
    generate_floyd_input(N, "input_v2.txt")