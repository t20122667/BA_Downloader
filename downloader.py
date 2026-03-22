import os
import pickle
import time
import requests
import re
import threading
import tkinter as tk
from urllib.parse import urlparse, parse_qs
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils import (
    write_log,
    save_failed_link,
    sizes_match,
    load_blacklist,
    run_on_ui_thread
)

# Глобальные переменные для отслеживания состояния загрузки
is_downloading_video = False
is_processing_links = False
current_video_url = None
current_video_name = None

# Глобальный флаг для сбора ссылок
is_collecting_links = False

# Глобальный флаг для постановки загрузки на паузу после завершения текущего видео
stop_downloading_flag = False


class DownloadProgressUI:
    """
    Потокобезопасная обёртка над Progressbar + Label.
    Все операции с виджетами выполняются только в главном потоке Tkinter.
    """

    def __init__(self, root, video_name):
        self.root = root
        self.video_name = video_name
        self.progress_bar = None
        self.progress_label = None
        self._created_event = threading.Event()
        self._destroyed = False

        run_on_ui_thread(self._create_widgets)
        self._created_event.wait(timeout=5)

    def _create_widgets(self):
        from tkinter import ttk

        self.progress_bar = ttk.Progressbar(
            self.root,
            orient="horizontal",
            length=400,
            mode="determinate"
        )
        self.progress_bar.pack(pady=(5, 10))

        self.progress_label = tk.Label(
            self.root,
            text=f"Загрузка {self.video_name}...",
            font=("Helvetica", 14)
        )
        self.progress_label.pack(pady=(5, 15))

        self._created_event.set()

    def _update_widgets(self, progress_percent, text):
        if self._destroyed:
            return
        if self.progress_bar is not None:
            self.progress_bar["value"] = progress_percent
        if self.progress_label is not None:
            self.progress_label.config(text=text)

    def update(self, progress_percent, text):
        run_on_ui_thread(self._update_widgets, progress_percent, text)

    def _destroy_widgets(self):
        self._destroyed = True
        if self.progress_label is not None:
            try:
                self.progress_label.destroy()
            except Exception:
                pass
            self.progress_label = None

        if self.progress_bar is not None:
            try:
                self.progress_bar.destroy()
            except Exception:
                pass
            self.progress_bar = None

    def destroy(self):
        run_on_ui_thread(self._destroy_widgets)


def transform_video_name(original_name):
    base_name, ext = os.path.splitext(original_name)

    # Пример: R0626HD-R1
    m = re.match(r'^[A-Za-z](\d{4})[A-Za-z0-9]+-([A-Za-z]\d+)$', base_name)
    if not m:
        return original_name

    participant_number = m.group(1)
    video_part = m.group(2)

    return f"{participant_number}-{video_part}{ext}"


def find_and_download_video(driver, root, video_link, download_folder, pause_event, blacklist):
    try:
        driver.get(video_link)
        m = re.search(r'person_number=(\d{4})', video_link)
        participant_id = m.group(1) if m else None

        from utils import cookies_path
        if os.path.exists(cookies_path):
            with open(cookies_path, "rb") as file:
                cookies = pickle.load(file)
                for cookie in cookies:
                    driver.add_cookie(cookie)
            driver.refresh()

        video_elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located(
                (By.XPATH, '//div[@id="playerinfo"]//a[@class="download_links_href"]')
            )
        )

        video_options = []
        for video_element in video_elements:
            vid_url = video_element.get_attribute("href")
            try:
                response = requests.head(vid_url, allow_redirects=True)
                size_bytes = int(response.headers.get("Content-Length", 0))
                video_options.append((vid_url, size_bytes))
            except Exception as e:
                write_log(f"Ошибка при определении размера для ссылки: {vid_url}. Ошибка: {e}", log_type="error")

        if not video_options:
            write_log(f"Не найдено доступных версий видео для ссылки: {video_link}", log_type="error")
            return False

        largest_video_url, largest_video_size = max(video_options, key=lambda x: x[1])
        original_video_name = largest_video_url.split("/")[-1].split("?")[0]
        video_name = transform_video_name(original_video_name)

        for num in blacklist:
            if num in video_name:
                write_log(f"Пропуск {video_name}: содержит число {num} из черного списка.", log_type="info")
                return False

        parsed = urlparse(video_link)
        media_id = parse_qs(parsed.query).get("media_id", [None])[0]
        progress_ui = DownloadProgressUI(root, video_name)

        try:
            return download_video(
                largest_video_url,
                download_folder,
                video_name,
                pause_event,
                progress_ui,
                blacklist,
                driver,
                media_id
            )
        finally:
            progress_ui.destroy()

    except Exception as e:
        write_log(f"Ошибка при обработке видео {video_link}: {e}", log_type="error")
        save_failed_link(video_link)
        return False


def download_video(video_url, output_folder, video_name, pause_event, progress_ui, blacklist, driver=None, media_id=None):
    for num in blacklist:
        if num in video_name:
            write_log(f"Пропуск {video_name}: содержит число {num} из черного списка.", log_type="info")
            return False

    global is_downloading_video, current_video_url, current_video_name
    is_downloading_video = True
    current_video_url = video_url
    current_video_name = video_name

    write_log(f"Начало обработки файла: {video_name}", log_type="video")

    try:
        response = requests.get(video_url, stream=True)
        response.raise_for_status()
        total_size = int(response.headers.get("content-length", 0))
        output_path = os.path.join(output_folder, video_name)

        if os.path.exists(output_path):
            existing_size = os.path.getsize(output_path)
            if sizes_match(existing_size, total_size, tolerance_percent=0.003):
                write_log(f"{video_name} уже скачано.", log_type="info")
                from utils import synchronize_file_dates
                if driver is not None and media_id is not None:
                    from utils import extract_page_release_date
                    page_release_ts = extract_page_release_date(driver, media_id)
                    synchronize_file_dates(output_path, page_release_ts)
                else:
                    synchronize_file_dates(output_path)
                write_log(f"Обработка {video_name} завершена.", log_type="info")
                return False
            else:
                write_log(f"{video_name}: размер не совпадает, перекачка.", log_type="info")
                write_log(f"Размер скачанного файла: {existing_size} байт", log_type="info")
                write_log(f"Ожидаемый размер файла: {total_size} байт", log_type="info")

        downloaded = 0
        chunk_size = 8192
        start_time = time.time()

        with open(output_path, "wb") as file:
            for chunk in response.iter_content(chunk_size):
                if not chunk:
                    continue

                pause_event.wait()
                file.write(chunk)
                downloaded += len(chunk)

                if total_size > 0:
                    progress_percent = int(downloaded / total_size * 100)
                else:
                    progress_percent = 0

                speed = downloaded / 1024 / max(time.time() - start_time, 1)
                progress_ui.update(
                    progress_percent,
                    f"{video_name}: {progress_percent}% @ {speed:.2f} KB/s"
                )

        write_log(f"{video_name} скачано успешно.", log_type="info")
        from utils import synchronize_file_dates
        if driver is not None and media_id is not None:
            from utils import extract_page_release_date
            page_release_ts = extract_page_release_date(driver, media_id)
            synchronize_file_dates(output_path, page_release_ts)
        else:
            synchronize_file_dates(output_path)

        write_log(f"Обработка {video_name} завершена.", log_type="info")
        return True

    except Exception as e:
        write_log(f"Ошибка при скачивании {video_name}: {e}", log_type="error")
        save_failed_link(video_url)
        return False

    finally:
        is_downloading_video = False


def collect_video_links(root, start_url, download_folder, search_pause_event, stop_on_empty_pages=False):
    global is_collecting_links
    if is_collecting_links:
        write_log("Сбор ссылок уже запущен!", log_type="info")
        return

    is_collecting_links = True

    video_links_file = "video_links.txt"
    existing_links = set()
    if os.path.exists(video_links_file):
        with open(video_links_file, "r", encoding="utf-8") as f:
            for line in f:
                link = line.strip()
                if link:
                    existing_links.add(link)

    links_collected = list(existing_links)

    parsed_url = urlparse(start_url)
    query_params = parse_qs(parsed_url.query)
    current_offset = int(query_params.get("offset", [0])[0])
    mode = query_params.get("mode", ["latest"])[0]

    base_url = f"https://beautifulagony.com/public/main.php?page=view&mode={mode}&offset={{}}"
    current_url = base_url.format(current_offset)

    empty_page_count = 0
    new_insert_count = 0

    try:
        from browser import driver

        while True:
            search_pause_event.wait()
            current_count_before = len(existing_links)

            write_log(f"Сбор ссылок, страница: {current_url}", log_type="info")
            driver.get(current_url)

            page_links = driver.find_elements(
                By.XPATH,
                '//a[contains(@href, "page=player&out=bkg&media")]'
            )

            if not page_links:
                write_log("На странице не найдено видео ссылок, завершаем сбор.", log_type="info")
                break

            blacklist = load_blacklist("blacklist.txt")

            for link_element in page_links:
                video_link = link_element.get_attribute("href")
                if not video_link:
                    continue
                video_link = video_link.strip()

                if video_link in existing_links:
                    write_log(f"Ссылка проигнорирована (уже существует): {video_link}", log_type="info")
                    continue

                try:
                    img_elem = link_element.find_element(By.TAG_NAME, "img")
                    alt_text = img_elem.get_attribute("alt")
                    real_number = alt_text.lstrip("#")
                except Exception as e:
                    write_log(f"Не удалось получить <img> или alt: {e}", log_type="error")
                    continue

                write_log(f"Найден реальный номер из alt: {real_number} для {video_link}", log_type="info")

                if real_number in blacklist:
                    write_log(
                        f"Ссылка проигнорирована (чёрный список – real_number={real_number}): {video_link}",
                        log_type="info"
                    )
                    continue

                if os.path.exists(video_links_file):
                    with open(video_links_file, "r", encoding="utf-8") as fr:
                        file_lines = fr.readlines()
                else:
                    file_lines = []

                file_lines.insert(new_insert_count, video_link + "\n")
                with open(video_links_file, "w", encoding="utf-8") as fw:
                    fw.writelines(file_lines)

                existing_links.add(video_link)
                links_collected.insert(new_insert_count, video_link)
                write_log(f"Новая ссылка добавлена (в начало файла): {video_link}", log_type="info")
                new_insert_count += 1

            new_links_count = len(existing_links) - current_count_before
            if new_links_count == 0:
                empty_page_count += 1
            else:
                empty_page_count = 0

            if stop_on_empty_pages and empty_page_count >= 3:
                write_log("Остановка поиска: 3 страницы подряд без новых ссылок.", log_type="info")
                break

            current_offset += 20
            current_url = base_url.format(current_offset)

        write_log(f"Сбор ссылок завершён, собрано {len(links_collected)} ссылок.", log_type="info")

    except Exception as e:
        write_log(f"Ошибка при сборе ссылок: {e}", log_type="error")

    finally:
        is_collecting_links = False

    return links_collected


def download_videos_sequential(root, download_folder, pause_event, stop_after_skip=False, direction="сначала"):
    global stop_downloading_flag

    try:
        with open("video_links.txt", "r", encoding="utf-8") as f:
            links = [line.strip() for line in f if line.strip()]
    except Exception as e:
        write_log(f"Ошибка при чтении файла ссылок: {e}", log_type="error")
        return

    if not links:
        write_log("Файл ссылок пуст.", log_type="info")
        return

    if direction == "с конца":
        links.reverse()

    write_log("Начало последовательной загрузки видео.", log_type="info")
    blacklist = load_blacklist("blacklist.txt")
    from browser import driver

    consecutive_skip_count = 0

    for link in links:
        pause_event.wait()
        result = download_video_sequential(driver, root, link, download_folder, pause_event, blacklist)

        if result is False:
            consecutive_skip_count += 1
        else:
            consecutive_skip_count = 0

        if stop_after_skip and consecutive_skip_count >= 10:
            write_log("Остановка загрузки: 10 подряд пропущенных видео.", log_type="info")
            break

        if stop_downloading_flag:
            write_log("Пауза загрузки по запросу.", log_type="info")
            pause_event.clear()
            pause_event.wait()
            stop_downloading_flag = False

    write_log("Последовательная загрузка завершена.", log_type="info")


def download_video_sequential(driver, root, video_link, download_folder, pause_event, blacklist):
    try:
        driver.get(video_link)

        m = re.search(r'person_number=(\d{4})', video_link)
        participant_id = m.group(1) if m else None

        from utils import cookies_path
        if os.path.exists(cookies_path):
            with open(cookies_path, "rb") as file:
                cookies = pickle.load(file)
                for cookie in cookies:
                    driver.add_cookie(cookie)
            driver.refresh()

        video_elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located(
                (By.XPATH, '//div[@id="playerinfo"]//a[@class="download_links_href"]')
            )
        )

        video_options = []
        for video_element in video_elements:
            vid_url = video_element.get_attribute("href")
            try:
                response = requests.head(vid_url, allow_redirects=True)
                size_bytes = int(response.headers.get("Content-Length", 0))
                video_options.append((vid_url, size_bytes))
            except Exception as e:
                write_log(f"Ошибка при определении размера для ссылки: {vid_url}. Ошибка: {e}", log_type="error")

        if not video_options:
            write_log(f"Не найдено доступных версий видео для ссылки: {video_link}", log_type="error")
            return False

        largest_video_url, largest_video_size = max(video_options, key=lambda x: x[1])
        original_video_name = largest_video_url.split("/")[-1].split("?")[0]
        video_name = transform_video_name(original_video_name)

        for num in blacklist:
            if num in video_name:
                write_log(f"Пропуск {video_name}: содержит число {num} из черного списка.", log_type="info")
                return False

        parsed = urlparse(video_link)
        media_id = parse_qs(parsed.query).get("media_id", [None])[0]
        output_path = os.path.join(download_folder, video_name)

        if os.path.exists(output_path):
            existing_size = os.path.getsize(output_path)
            if sizes_match(existing_size, largest_video_size, tolerance_percent=0.003):
                write_log(f"{video_name} уже скачано.", log_type="info")
                from utils import synchronize_file_dates, extract_page_release_date
                page_release_ts = extract_page_release_date(driver, media_id)
                synchronize_file_dates(output_path, page_release_ts)
                write_log(f"Обработка {video_name} завершена.", log_type="info")
                return False

        progress_ui = DownloadProgressUI(root, video_name)

        try:
            return download_video(
                largest_video_url,
                download_folder,
                video_name,
                pause_event,
                progress_ui,
                blacklist,
                driver,
                media_id
            )
        finally:
            progress_ui.destroy()

    except Exception as e:
        write_log(f"Ошибка при обработке видео {video_link}: {e}", log_type="error")
        save_failed_link(video_link)
        return False