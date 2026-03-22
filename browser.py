import threading
import pickle

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from utils import write_log, cookies_path, load_config, run_on_ui_thread

# Глобальная переменная для драйвера
driver = None


def save_cookies(current_driver):
    with open(cookies_path, "wb") as file:
        pickle.dump(current_driver.get_cookies(), file)


def authorize(timer_label, check_button):
    def browser_thread():
        global driver
        config = load_config()
        username = config.get("username", "")
        password = config.get("password", "")

        try:
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
            if driver:
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

        current_url = driver.current_url.rstrip("/")

        if current_url == "https://beautifulagony.com/public/main.php":
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