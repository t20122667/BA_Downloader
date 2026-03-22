import os
import pickle
import re
import threading
import time
import tkinter as tk
from urllib.parse import urlparse, parse_qs

import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from utils import (
    write_log,
    save_failed_link,
    sizes_match,
    load_blacklist,
    run_on_ui_thread,
)

# Глобальные переменные для отслеживания состояния загрузки
is_downloading_video = False
current_video_url = None
current_video_name = None

# Глобальный флаг для сбора ссылок
is_collecting_links = False

# Глобальный флаг для остановки после текущего видео
stop_downloading_flag = False


def request_stop_after_current():
    global stop_downloading_flag
    stop_downloading_flag = True
    write_log("Запрошена остановка после текущего видео.", log_type="info")


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

        if not self._created_event.wait(timeout=5):
            write_log("Не удалось вовремя создать элементы прогресса.", log_type="error")

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
    match = re.match(r"^[A-Za-z](\d{4})[A-Za-z0-9]+-([A-Za-z]\d+)$", base_name)
    if not match:
        return original_name

    participant_number = match.group(1)
    video_part = match.group(2)

    return f"{participant_number}-{video_part}{ext}"


def _load_cookies_into_driver(driver):
    from utils import cookies_path

    if not os.path.exists(cookies_path):
        return

    with open(cookies_path, "rb") as file:
        cookies = pickle.load(file)

    for cookie in cookies:
        try:
            driver.add_cookie(cookie)
        except Exception:
            pass

    driver.refresh()


def _prepare_video_download(driver, video_link, blacklist):
    driver.get(video_link)
    _load_cookies_into_driver(driver)

    video_elements = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located(
            (By.XPATH, '//div[@id="playerinfo"]//a[@class="download_links_href"]')
        )
    )

    video_options = []
    for video_element in video_elements:
        vid_url = video_element.get_attribute("href")
        try:
            response = requests.head(vid_url, allow_redirects=True, timeout=20)
            size_bytes = int(response.headers.get("Content-Length", 0))
            video_options.append((vid_url, size_bytes))
        except Exception as e:
            write_log(f"Ошибка при определении размера для ссылки: {vid_url}. Ошибка: {e}", log_type="error")

    if not video_options:
        write_log(f"Не найдено доступных версий видео для ссылки: {video_link}", log_type="error")
        return None

    largest_video_url, largest_video_size = max(video_options, key=lambda x: x[1])
    original_video_name = largest_video_url.split("/")[-1].split("?")[0]
    video_name = transform_video_name(original_video_name)

    for num in blacklist:
        if num in video_name:
            write_log(f"Пропуск {video_name}: содержит число {num} из черного списка.", log_type="info")
            return None

    parsed = urlparse(video_link)
    media_id = parse_qs(parsed.query).get("media_id", [None])[0]

    return {
        "largest_video_url": largest_video_url,
        "largest_video_size": largest_video_size,
        "video_name": video_name,
        "media_id": media_id,
    }


def find_and_download_video(driver, root, video_link, download_folder, pause_event, blacklist):
    try:
        prepared = _prepare_video_download(driver, video_link, blacklist)
        if prepared is None:
            return False

        progress_ui = DownloadProgressUI(root, prepared["video_name"])
        try:
            return download_video(
                prepared["largest_video_url"],
                download_folder,
                prepared["video_name"],
                pause_event,
                progress_ui,
                blacklist,
                driver,
                prepared["media_id"]
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
        response = requests.get(video_url, stream=True, timeout=60)
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

                progress_percent = int(downloaded / total_size * 100) if total_size > 0 else 0
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


def collect_video_links(start_url, search_pause_event, stop_on_empty_pages=False):
    global is_collecting_links

    if is_collecting_links:
        write_log("Сбор ссылок уже запущен!", log_type="info")
        return

    is_collecting_links = True

    try:
        from browser import driver

        if driver is None:
            write_log("Браузер не инициализирован. Сначала пройдите авторизацию.", log_type="error")
            return

        video_links_file = "video_links.txt"
        existing_links_in_file = []

        if os.path.exists(video_links_file):
            with open(video_links_file, "r", encoding="utf-8") as f:
                existing_links_in_file = [line.strip() for line in f if line.strip()]

        existing_links = set(existing_links_in_file)
        session_new_links = []

        parsed_url = urlparse(start_url)
        query_params = parse_qs(parsed_url.query)
        current_offset = int(query_params.get("offset", [0])[0])
        mode = query_params.get("mode", ["latest"])[0]

        base_url = f"https://beautifulagony.com/public/main.php?page=view&mode={mode}&offset={{}}"
        current_url = base_url.format(current_offset)

        empty_page_count = 0

        while True:
            search_pause_event.wait()
            current_count_before = len(existing_links)

            write_log(f"Сбор ссылок, страница: {current_url}", log_type="page")
            driver.get(current_url)

            page_links = driver.find_elements(
                By.XPATH,
                '//a[contains(@href, "page=player&out=bkg&media")]'
            )

            if not page_links:
                write_log("На странице не найдено видео ссылок, завершаем сбор.", log_type="info")
                break

            blacklist = load_blacklist("blacklist.txt")
            page_new_links = []

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
                    alt_text = (img_elem.get_attribute("alt") or "").strip()
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

                existing_links.add(video_link)
                page_new_links.append(video_link)
                write_log(f"Новая ссылка добавлена: {video_link}", log_type="info")

            if page_new_links:
                session_new_links.extend(page_new_links)
                final_links = session_new_links + existing_links_in_file
                with open(video_links_file, "w", encoding="utf-8") as fw:
                    fw.write("\n".join(final_links) + ("\n" if final_links else ""))

            new_links_count = len(existing_links) - current_count_before
            empty_page_count = empty_page_count + 1 if new_links_count == 0 else 0

            if stop_on_empty_pages and empty_page_count >= 3:
                write_log("Остановка поиска: 3 страницы подряд без новых ссылок.", log_type="info")
                break

            current_offset += 20
            current_url = base_url.format(current_offset)

        write_log(f"Сбор ссылок завершён, собрано {len(existing_links)} ссылок.", log_type="info")
        return list(existing_links)

    except Exception as e:
        write_log(f"Ошибка при сборе ссылок: {e}", log_type="error")
        return None

    finally:
        is_collecting_links = False


def download_videos_sequential(root, download_folder, pause_event, stop_after_skip=False, direction="сначала"):
    global stop_downloading_flag

    try:
        from browser import driver

        if driver is None:
            write_log("Браузер не инициализирован. Сначала пройдите авторизацию.", log_type="error")
            return

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

    stop_downloading_flag = False

    write_log("Начало последовательной загрузки видео.", log_type="info")
    blacklist = load_blacklist("blacklist.txt")
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
            write_log("Загрузка остановлена по запросу после завершения текущего видео.", log_type="info")
            stop_downloading_flag = False
            break

    write_log("Последовательная загрузка завершена.", log_type="info")


def download_video_sequential(driver, root, video_link, download_folder, pause_event, blacklist):
    try:
        prepared = _prepare_video_download(driver, video_link, blacklist)
        if prepared is None:
            return False

        output_path = os.path.join(download_folder, prepared["video_name"])

        if os.path.exists(output_path):
            existing_size = os.path.getsize(output_path)
            if sizes_match(existing_size, prepared["largest_video_size"], tolerance_percent=0.003):
                write_log(f"{prepared['video_name']} уже скачано.", log_type="info")
                from utils import synchronize_file_dates, extract_page_release_date

                page_release_ts = extract_page_release_date(driver, prepared["media_id"])
                synchronize_file_dates(output_path, page_release_ts)
                write_log(f"Обработка {prepared['video_name']} завершена.", log_type="info")
                return False

        progress_ui = DownloadProgressUI(root, prepared["video_name"])
        try:
            return download_video(
                prepared["largest_video_url"],
                download_folder,
                prepared["video_name"],
                pause_event,
                progress_ui,
                blacklist,
                driver,
                prepared["media_id"]
            )
        finally:
            progress_ui.destroy()

    except Exception as e:
        write_log(f"Ошибка при обработке видео {video_link}: {e}", log_type="error")
        save_failed_link(video_link)
        return False