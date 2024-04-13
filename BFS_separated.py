from collections import deque
import json
import time
import pymysql

# bfs
def bfs(graph, start, end):
    if start == end:
        return [start]
    visited = set()
    queue = deque([(start, [start])])

    while queue:
        vertex, path = queue.popleft()

        if vertex not in visited:
            visited.add(vertex)

            for neighbor in graph[vertex]:
                if str(neighbor) == end:
                    return path + [neighbor]

                if neighbor not in visited:
                    queue.append((str(neighbor), path + [neighbor]))

    return None  # 경로가 없는 경우

# JSON 파일에서 그래프 데이터 읽기
def load_graph_from_json(filename):
    with open(filename, 'r') as f:
        graph_data = json.load(f)
    return graph_data

# JSON 파일 경로
people_to_movie_json_filename = 'people_to_movie.json'
movie_to_people_json_filename = 'movie_to_people.json'
config_json_filename = 'config.json'

# 그래프 데이터 로드
people_to_movie = load_graph_from_json(people_to_movie_json_filename)
movie_to_people = load_graph_from_json(movie_to_people_json_filename)

# MySQL 연결 정보 로드
with open(config_json_filename, 'r') as f:
    config = json.load(f)

# 시작 노드와 끝나는 노드 입력받기
start_node = str(input("Enter the start node: "))
end_node = str(input("Enter the end node: "))

# 시작시간
start_time = time.time()

# MySQL 연결
conn = pymysql.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database=config['database'],
        port=config['port'],
        cursorclass=pymysql.cursors.DictCursor
    )

# BFS 실행
path = bfs(people_to_movie, start_node, end_node)
path = list(map(str, path))

# 끝나는시간
end_time = time.time()
elapsed_time = end_time - start_time

# 결과 출력
if path:
    print(f"Shortest path from {start_node} to {end_node}: {' -> '.join(path)}")
    print(f"Elapsed time: {elapsed_time:.6f}sec")
else:
    print(f"No path found from {start_node} to {end_node}")

# 연결 및 커서 닫기
conn.close()
