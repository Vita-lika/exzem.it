import os
import json
from pathlib import Path

def analyze_logs(directory):
    results = []
    error_keywords = ("ERROR", "EXCEPTION")
    
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.log'):
                file_path = os.path.join(root, file)
                error_count = 0
                last_error = None
                
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        for line in f:
                            if any(keyword in line for keyword in error_keywords):
                                error_count += 1
                                last_error = line.strip()
                except Exception:
                    # Пропускаем файлы, которые не удалось прочитать
                    continue
                
                if error_count > 0:
                    results.append({
                        "filename": file_path,
                        "error_count": error_count,
                        "last_error": last_error
                    })
    
    return results
    
    # Сохраняем отчёт в JSON-файл
    with open("log_report.json", "w", encoding="utf-8") as out_file:
        json.dump(report, out_file, ensure_ascii=False, indent=4)
    
    print(f"Анализ завершён. Найдено {len(report)} файлов с ошибками. Отчёт сохранён в log_report.json")