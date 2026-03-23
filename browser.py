import pickle
import threading

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from utils import write_log, cookies_path, load_config, run_on_ui_thread

# Глобальная переменная для драйвера
driver = None

# Защита от параллельного использования одного Selenium driver
_driver_state_lock = threading.Lock()
_driver_session_owner = None


def save_cookies(current_driver):
    with open(cookies_path, "wb") as file:
        pickle.dump(current_driver.get_cookies(), file)


def begin_driver_session(task_name: str, quiet: bool = False) -> bool:
    """
    Регистрирует эксклюзивное использование driver для одной задачи.
    Возвращает True, если сессия захвачена успешно.
    """
    global _driver_session_owner, driver

    with _driver_state_lock:
        if driver is None:
            write_log("Браузер не запущен. Сначала пройдите авторизацию.", log_type="error")
            return False

        if _driver_session_owner is not None:
            if not quiet:
                write_log(
                    f"Браузер уже используется задачей '{_driver_session_owner}'. "
                    f"Дождитесь её завершения.",
                    log_type="info"
                )
            return False

        _driver_session_owner = task_name
        return True


def end_driver_session(task_name: str | None = None):
    """
    Освобождает эксклюзивную сессию использования driver.
    """
    global _driver_session_owner

    with _driver_state_lock:
        if task_name is None or _driver_session_owner == task_name:
            _driver_session_owner = None


def is_driver_busy() -> bool:
    with _driver_state_lock:
        return _driver_session_owner is not None


def authorize(timer_label, check_button):
    def browser_thread():
        global driver

        config = load_config()
        username = config.get("username", "")
        password = config.get("password", "")

        try:
            if is_driver_busy():
                write_log(
                    "Нельзя переоткрыть браузер во время активного сбора ссылок или загрузки.",
                    log_type="error"
                )
                return

            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = None

            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service)
            driver.get("https://beautifulagony.com/public/main.php?page=login")

            driver.find_element(By.NAME, "username").send_keys(username)
            driver.find_element(By.NAME, "userpass").send_keys(password)
            driver.find_element(By.NAME, "userpass").send_keys(Keys.RETURN)

            run_on_ui_thread(
                timer_label.configure,
                text="Решите капчу и нажмите 'Проверить авторизацию'!"
            )
            run_on_ui_thread(check_button.configure, state="normal")

        except Exception as e:
            write_log(f"Ошибка запуска браузера или авторизации: {e}", log_type="error")
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = None

    threading.Thread(target=browser_thread, daemon=True).start()


def check_authorization(timer_label, on_success=None):
    global driver

    try:
        if driver is None:
            write_log("Браузер не запущен. Сначала нажмите 'Пройти авторизацию'.", log_type="error")
            return False

        current_url = (driver.current_url or "").rstrip("/")

        # Более мягкая и надёжная проверка, чем жёсткое равенство одной строке URL
        is_logged_in = (
            current_url.startswith("https://beautifulagony.com/public/main.php")
            and "page=login" not in current_url
        )

        if is_logged_in:
            save_cookies(driver)
            write_log("Авторизация завершена. Cookies сохранены.", log_type="info")
            run_on_ui_thread(timer_label.configure, text="Авторизация подтверждена.")
            if on_success is not None:
                run_on_ui_thread(on_success)
            return True

        write_log("Не удалось пройти авторизацию. Проверьте капчу.", log_type="error")
        return False

    except Exception as e:
        write_log(f"Ошибка проверки авторизации: {e}", log_type="error")
        return False