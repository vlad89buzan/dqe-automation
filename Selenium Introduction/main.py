import time
import os
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


class SeleniumWebDriverContextManager:
    def __init__(self, headless: bool = False, timeout: int = 10):
        self.headless = headless
        self.timeout = timeout
        self.driver: WebDriver | None = None
        self.wait: WebDriverWait | None = None

    def __enter__(self):
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument("--headless=new")

        self.driver = webdriver.Chrome(options=options)
        self.driver.maximize_window()
        self.wait = WebDriverWait(self.driver, self.timeout)

        return self.driver

    def __exit__(self, exc_type, exc_value, traceback):
        if self.driver is not None:
            try:
                self.driver.quit()
            except Exception as e:
                print(f"Error while quitting WebDriver: {e}")
        return False


def ensure_folder(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)


def safe_find_element(parent, by, value):
    """Helper to safely find an element, return None if not found."""
    try:
        return parent.find_element(by, value)
    except NoSuchElementException:
        return None


def extract_chart_data(chart_element):
    """Extract data from pyecharts doughnut slices."""
    data = []
    slices = chart_element.find_elements(By.CSS_SELECTOR, "g.slice")
    for slice_ in slices:
        tspan_elements = slice_.find_elements(By.CSS_SELECTOR, "g.slicetext text tspan")
        if len(tspan_elements) >= 2:
            data.append({
                "Facility Type": tspan_elements[0].text.strip(),
                "Min Average Time Spent": tspan_elements[1].text.strip()
            })
    return pd.DataFrame(data)


def safe_screenshot(driver, element, path):
    """Take a screenshot safely; fallback to full page if element has zero size."""
    try:
        size = element.size
        if size['width'] > 0 and size['height'] > 0:
            element.screenshot(path)
        else:
            print(f"Element has zero size, taking full page screenshot: {path}")
            driver.save_screenshot(path)
    except Exception as e:
        print(f"Failed to take screenshot {path}: {e}")


def interact_with_doughnut_chart(driver, wait):
    ensure_folder("doughnut")

    # Locate the doughnut chart container
    try:
        chart = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "pielayer"))
        )
    except TimeoutException:
        print("Doughnut chart not found.")
        return

    screenshot_counter = 0
    safe_screenshot(driver, chart, f"doughnut/screenshot{screenshot_counter}.png")
    extract_chart_data(chart).to_csv(f"doughnut/doughnut{screenshot_counter}.csv", index=False)
    screenshot_counter += 1

    # Locate filter options
    filtr = safe_find_element(driver, By.CLASS_NAME, "legend")
    if not filtr:
        print("Filters not found.")
        return

    filters = filtr.find_elements(By.CLASS_NAME, "traces")
    print(f"Found {len(filters)} filters")

    # Iterate through filters
    for filter_option in filters:
        try:
            filter_option.click()
            time.sleep(1)  # wait for chart to update
            safe_screenshot(driver, chart, f"doughnut/screenshot{screenshot_counter}.png")
            extract_chart_data(chart).to_csv(f"doughnut/doughnut{screenshot_counter}.csv", index=False)
            screenshot_counter += 1
        except Exception as e:
            print(f"Failed to apply filter or capture chart: {e}")


def interact_with_table(driver, wait):
    try:
        table = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "table")))
        print('Table found')
    except TimeoutException:
        print("ERROR: Table not found!")
        return

    data = {}
    columns = table.find_elements(By.CLASS_NAME, "y-column")

    for col_index, column in enumerate(columns):
        header_block = safe_find_element(column, By.ID, "header")
        header_text = header_block.text.strip() if header_block else f"Column_{col_index + 1}"

        column_cells = []
        column_blocks = column.find_elements(By.CSS_SELECTOR, "g.column-block")
        for block in column_blocks:
            if block.get_attribute("id") == "header":
                continue
            cells_container = safe_find_element(block, By.CLASS_NAME, "column-cells")
            if cells_container:
                cells = cells_container.find_elements(By.CLASS_NAME, "column-cell")
                for cell in cells:
                    column_cells.append(cell.text.strip())

        data[header_text] = column_cells

    df = pd.DataFrame({k: pd.Series(v) for k, v in data.items()})
    df.to_csv("table.csv", index=False)
    print("Table saved to table.csv")


if __name__ == "__main__":
    with SeleniumWebDriverContextManager() as driver:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        html_file_path = os.path.join(base_dir, "report.html")
        file_path = "file:///" + html_file_path.replace("\\", "/")
        driver.get(file_path)
        wait = WebDriverWait(driver, 10)

        interact_with_table(driver, wait)
        interact_with_doughnut_chart(driver, wait)
