import os
import sqlite3
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Инициализация базы данных
def init_db(db_path="file_hashes.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS file_hashes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            filepath TEXT NOT NULL,
            sha256 TEXT NOT NULL UNIQUE
        )
    """)
    conn.commit()
    conn.close()

# Вычисление SHA256 хеша файла
def compute_sha256(filepath):
    hash_sha256 = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return str(filepath), hash_sha256.hexdigest()
    except Exception as e:
        print(f"Ошибка при обработке {filepath}: {e}")
        return str(filepath), None

# Сохранение результата в БД
def save_to_db(results, db_path="file_hashes.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    for filepath, sha256 in results:
        if sha256 is not None:
            filename = os.path.basename(filepath)
            cursor.execute(
                "INSERT OR REPLACE INTO file_hashes (filename, filepath, sha256) VALUES (?, ?, ?)",
                (filename, filepath, sha256)
            )
    conn.commit()
    conn.close()

# Основная функция
def main(directory, max_workers=4):
    directory = Path(directory)
    if not directory.is_dir():
        print(f"Путь {directory} не является директорией.")
        return

    # Получаем список всех файлов
    file_paths = [p for p in directory.rglob("*") if p.is_file()]

    if not file_paths:
        print("Файлы не найдены.")
        return

    init_db()

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_path = {executor.submit(compute_sha256, fp): fp for fp in file_paths}
        for future in as_completed(future_to_path):
            filepath, sha256 = future.result()
            results.append((filepath, sha256))

    save_to_db(results)
    print(f"Обработано {len(results)} файлов. Результаты сохранены в file_hashes.db")

# Пример запуска
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Использование: python hash_files.py <путь_к_директории>")
        sys.exit(1)
    main(sys.argv[1])
