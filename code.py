import os

# Определить путь к папке, где находится сам скрипт
project_dir = os.path.dirname(os.path.abspath(__file__))

# Файл для сохранения кода
output_file = os.path.join(project_dir, "project_code.txt")

# Список файлов, которые нужно записать
files = ["main.py", "gui.py", "downloader.py", "browser.py", "utils.py"]

# Запись кода в файл
with open(output_file, "w", encoding="utf-8") as out_f:
    for file in files:
        file_path = os.path.join(project_dir, file)
        if os.path.exists(file_path):
            out_f.write(f"{file}:\n")
            with open(file_path, "r", encoding="utf-8") as f:
                out_f.write(f.read())
            out_f.write("\n\n")  # Разделитель между файлами
        else:
            print(f"⚠️ Файл {file} не найден!")

# Открытие файла после завершения
os.startfile(output_file)

print(f"Код файлов сохранён в {output_file} и открыт в текстовом редакторе.")
